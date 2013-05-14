-- Complete this job
function QsomeJob:complete(now, worker, queue, data, ...)
    -- We're ignoring the queue provided by the worker for now. At some point
    -- we may want to do something like verify that they're in the same super
    -- queue, but I think we're ok for now
    local rqueue = redis.call('hget', 'ql:j:' .. self.jid, 'queue')

    -- Read in all the optional parameters. The ... options are of the form
    -- `key`, `value`, `key`, `value`, so this snippet is designed to iterate
    -- over all the pairs and make a map of the keys to the values.
    local options = {}
    for i = 1, #arg, 2 do options[arg[i]] = arg[i + 1] end
    local nextq   = options['next']
    local delay   = assert(tonumber(options['delay'] or 0))
    local depends = assert(cjson.decode(options['depends'] or '[]'),
        'Complete(): Arg "depends" not JSON: ' .. tostring(options['depends']))

    -- If we have a 'next' queue, then we actually need to catch that and
    -- override it here
    if nextq then
        -- Remove the next queue
        options['next'] = nil
        options['delay'] = nil

        -- Save all our data before we change it
        local job = self:data()
        local resp = Qless.job(self.jid):complete(now, worker, rqueue, data)
        if resp == 'complete' then
            Qsome.queue(nextq):put(now, self.jid, job.klass, job.hash,
                data, delay,
                'retries', job.retries,
                'tags', cjson.encode(job.tags),
                'priority', job.priority,
                'depends', options['depends'] or '[]')
            return 'waiting'
        else
            return resp
        end
    end

    -- If we didn't have a nextq, then we'll just do it normal style
    return Qless.job(self.jid):complete(now, worker, rqueue, data, unpack(arg))
end

-- Return the data associated with this particular job
function QsomeJob:data()
    local data = Qless.job(self.jid):data()
    if data ~= nil then
        -- Now augment the job data with a hash property
        data['hash'] = self:hash()
    end
    return data
end

--! @brief Return the hash of the job
function QsomeJob:hash(value)
    if value == nil then
        return redis.call('hset', QsomeJob.ns .. self.jid, 'hash', value)
    else
        return redis.call('hget', QsomeJob.ns .. self.jid, 'hash')
    end
end

--! @brief Heartbeat a job
function QsomeJob:heartbeat(now, worker, data)
    return Qless.job(self.jid):heartbeat(now, worker, data)
end
