-------------------------------------------------------------------------------
-- QsomeQueue Definition
-------------------------------------------------------------------------------
-- Auxilary keys
--
-- :available -- stores a sorted set of the subqueues available to pick work
--      from. Each element's score is the time when the last piece of work from
--      that queue was completed, and so are sorted in order of which should be
--      used next. This implements a least-recently used sort of order
--
-- :active -- a hash of the counts of jobs running in various subqueues. The
--      reason that we need this around is that when resizing, we can get the
--      case where a subqueue might have two active jobs. Therefore we must
--      keep a count around so that we don't immediately put it back into the
--      queue for work.

--! @brief Create a queue with the provided number of subqueues
--! @param subqueues - the number of subqueues in this queue
function QsomeQueue:create(subqueues)
end

--! @brief Destroy the queue
function QsomeQueue:destroy()
    error('Queue.destroy(): Method intentionally unimplemented')
end

function QsomeQueue:prefix()
    return QsomeQueue.ns .. self.name
end

--! @brief Pop the next subqueue we should pop from. This also finds any
--      subqueues that have jobs that have jobs with lost locks
function QsomeQueue:pop_subqueue(now)
    -- If there is no queue of our subqueues, then we'll instantiate it
    local key = self:prefix()..':available'
    local available = redis.call('lrange', key, 0, -1)
    if #available == 0 then
        -- If this is empty, then there are two possibilities -- we've not
        -- initialized it, or there are legitimately no queues available. So we
        -- must check the 'active'
        available = self:subqueues()
        redis.call('lpush', key, unpack(available))
    end

    -- Each queue should have a configurable rate limit on the number of jobs
    -- that can be in flight in a queue at any given time.
    local limit = tonumber(self:config('concurrency') or 1)
    local i = 0
    return  function()
                while i < #available do
                    i = i + 1
                    local subqueue = redis.call('rpoplpush', key, key)
                    local running = Qless.queue(subqueue).locks.running(now)
                    if running < limit then
                        return subqueue
                    end
                end
            end
end

--! @brief Pop the provided number of jobs from the queue
--! @param count - number of jobs to pop
function QsomeQueue:pop(now, worker, count)
    -- Ensure that count is in fact a number
    count = assert(tonumber(count),
        'Queue.pop(): Count not a number: ' .. tostring(count))
    local jids = {}
    local iter = self:pop_subqueue(now)
    local subqueue = iter()
    while (#jids < count) and subqueue do
        local _jids = Qless.queue(subqueue):pop(now, worker, 1)
        if #_jids then
            local jid = _jids[1]
            table.insert(jids, jid)
        end
        subqueue = iter()
    end
    return jids
end

--! @brief Peek at the next jobs available
--! @param count -- number of jobs to peek
function QsomeQueue:peek(now, count)
    -- Ensure that count is in fact a number
    count = assert(tonumber(count),
        'Queue.peek(): Count not a number: ' .. tostring(count))
    local jids = {}
    local key = self:prefix()..':available'
    local available = redis.call('lrange', key, 0, -1)
    for i, subqueue in ipairs(available) do
        if #jids >= count then
            break
        end
        local _jids = Qless.queue(subqueue):peek(now, 1)
        if #_jids then
            local jid = _jids[1]
            table.insert(jids, jid)
        end
    end
    return jids
end

--! @brief Enqueue a job to be executed
--! @param now - the current timestamp in seconds since epoch
--! @param jid - job id
--! @param klass - job's class name
--! @param hash - integer hash associated with the job
--! @param data - json-encoded data for the job
--! @param delay - seconds the job must wait before running
function QsomeQueue:put(now, jid, klass, hash, data, delay, ...)
    -- The first order of real business is to determine which subqueue this
    -- job will go into
    local count = tonumber(self:config('size') or 1)
    local hash  = assert(tonumber(hash),
        'Queue.put(): Hash missing or not a number: ' .. tostring(hash))
    local subqueue = self.name .. '-' .. tostring(hash % count + 1)
    if not redis.call('zscore', Qsome.ns .. 'queues', self.name) then
        redis.call('zadd', Qsome.ns .. 'queues', now, self.name)
    end
    local response = Qless.queue(subqueue):put(
        now, jid, klass, data, delay, unpack(arg))
    if response then
        -- Qless doesn't save the hash into the job data, so we must do that
        -- ourselves
        redis.call('hset', 'ql:j:' .. jid, 'hash', hash)
    end
    return response
end

--! @brief Change the number of subqueues in this queue
--! @param size - new number of subqueues
function QsomeQueue:resize(size)
    -- Ensure that the size is a number
    size = assert(tonumber(size),
        'Queue.resize(): Size is not a number: ' .. tostring(size))
    -- Ensure that the size is positive
    if size >= 1 then
        -- Let's get the list of subqueues as it exists now, and then we'll
        -- update the size and get the new list of subqueues
        local old_subqueues = self:subqueues()
        self:config('size', size)

        local names = { 'work', 'recur', 'scheduled', 'depends', 'locks' }
        for i, name in ipairs(names) do
            local jid_map = {}
            for j, subqueue in ipairs(old_subqueues) do
                jid_map[subqueue] = redis.call('zrange',
                    Qless.queue(subqueue):prefix(name), 0, -1, 'withscores')
            end

            for subqueue, jids in pairs(jid_map) do
                local zrem = {}
                for k=1,#jids,2 do
                    local jid   = jids[k]
                    local score = jids[k+1]
                    -- Find out which subqueue it's moving into, and move it
                    local to = self:subqueues(Qsome.job(jid):hash(), size)

                    -- If the job is staying in the same subqueue, then keep
                    -- it there and don't do anything with it
                    if to ~= subqueue then
                        redis.call(
                            'zadd', Qless.queue(to):prefix(name), score, jid)
                        redis.call('hset', 'ql:j:' .. jid, 'queue', to)
                        table.insert(zrem, jid)
                        if #zrem > 100 then
                            redis.call('zrem',
                                Qless.queue(subqueue):prefix(name),
                                unpack(zrem))
                            zrem = {}
                        end
                    end
                end

                -- Now remove all these jobs from the last queue it was in
                if #zrem > 0 then
                    redis.call('zrem',
                        Qless.queue(subqueue):prefix(name), unpack(zrem))
                end
            end
        end

        -- We have to reset the available subqueues
        redis.call('del', self:prefix()..':available')
    else
        error('Queue.resize(): Size must be >= 1: ' .. tostring(size))
    end
end

--! @brief List the subqueues of the provided queue
function QsomeQueue:subqueues(hash, count)
    -- The subqueues of a particular queue are suffixed with this queue name
    -- and 1 through the number of subqueues this queue has
    if hash == nil then
        local size = tonumber(self:config('size') or 1)
        local response = {}
        for i=1,size do
            table.insert(response, self.name .. '-' .. tostring(i))
        end
        return response
    else
        count = count or tonumber(self:config('size') or 1)
        return self.name .. '-' .. tostring(hash % count + 1)
    end
end

--! @brief Return all the settings for this queue
function QsomeQueue:config(key, value)
    if key ~= nil then
        if value then
            return redis.call(
                'hset', self:prefix()..':config', key, value)
        else
            return redis.call('hget', self:prefix()..':config', key)
        end
    else
        local response = {}
        local reply = redis.call('hgetall', self:prefix()..':config')
        for i = 1, #reply, 2 do
            response[reply[i]] = reply[i + 1]
        end
        return response
    end
end

--! @brief return information about the jobs in this queue
function QsomeQueue:jobs(now)
    local subqueues = self:subqueues()
    local response = {}
    for i, qname in ipairs(subqueues) do
        local stats = Qless.queues(now, qname)
        for k, v in pairs(stats) do
            if response[k] == nil then
                response[k] = stats[k]
            else
                response[k] = response[k] + stats[k]
            end
        end
    end
    return response
end

--! @brief Return the length of the queue
function QsomeQueue:length()
    local total = 0
    for i, qname in ipairs(self:subqueues()) do
        total = total + Qless.queue(qname):length()
    end
    return total
end

--! @brief Get some stats
function QsomeQueue:stats(now, date)
    local subqueues = self:subqueues()
    local results = {
        failed   = 0,
        failures = 0,
        retries  = 0,
        wait     = nil,
        run      = nil
    }
    for i, subqueue in ipairs(subqueues) do
        local stats = Qless.queue(subqueue):stats(now, date)
        results['failed']   = results['failed']   + stats['failed']
        results['failures'] = results['failures'] + stats['failures']
        results['retries']  = results['retries']  + stats['retries']
        if results['wait'] == nil then
            results['wait'] = stats['wait']
            results['run' ] = stats['run' ]
        else
            -- Wait stats
            results['wait']['mean'] = 
                (results['wait']['mean'] * results['wait']['count']) +
                (  stats['wait']['mean'] *   stats['wait']['count'])
            results['wait']['count'] = 
                results['wait']['count'] + stats['wait']['count']
            if (results['wait']['count'] > 0) then
                results['wait']['mean'] =
                    results['wait']['mean'] / results['wait']['count']
            end
            -- Run stats
            results['run']['mean'] = 
                (results['run']['mean'] * results['run']['count']) +
                (  stats['run']['mean'] *   stats['run']['count'])
            results['run']['count'] = 
                results['run']['count'] + stats['run']['count']
            if (results['run']['count'] > 0) then
                results['run']['mean'] =
                    results['run']['mean'] / results['run']['count']
            end

            -- Merge the histograms
            for j=1,#stats['wait']['histogram'] do
                results['wait']['histogram'][j] = (results['wait']['histogram'][j] or 0) + stats['wait']['histogram'][j]
            end
            for j=1,#stats['run']['histogram'] do
                results['run']['histogram'][j] = (results['run']['histogram'][j] or 0) + stats['run']['histogram'][j]
            end
        end
    end
    return results
end
