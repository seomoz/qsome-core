-- Qsome. Like qless, but a little more
local Qsome = {
    -- The Qsome namespace
    ns = 'qs:'
}

-- Forward declaration of QsomeQueue
local QsomeQueue = {
    -- The Qsome queues namespace
    ns = Qsome.ns .. 'q:'
}
QsomeQueue.__index = QsomeQueue

-- Forward declaration of QsomeJob
local QsomeJob = {
    -- The Qsome job namespace
    ns = 'ql:j:'
}
QsomeJob.__index = QsomeJob

-------------------------------------------------------------------------------
-- Factory functions
-------------------------------------------------------------------------------
function Qsome.queue(name)
    assert(name, 'Queue(): Arg "name" missing')
    local queue = {}
    setmetatable(queue, QsomeQueue)
    queue.name = name
    return queue
end

function Qsome.job(jid)
    assert(jid, 'Job(): Arg "jid" missing')
    local job = {}
    setmetatable(job, QsomeJob)
    job.jid = jid
    return job
end

-------------------------------------------------------------------------------
-- Some methods exposed at the global level
-------------------------------------------------------------------------------
--! @brief Return a list of all the known queues
function Qsome.queues(now, queue)
    if queue then
        local results = {
            stalled   = 0,
            waiting   = 0,
            running   = 0,
            scheduled = 0,
            depends   = 0,
            recurring = 0
        }
        local subqueues = Qsome.queue(queue):subqueues()
        for i, subqueue in ipairs(subqueues) do
            local counts = Qless.queues(now, subqueue)
            for key, value in pairs(counts) do
                if tonumber(value) then
                    results[key] = results[key] + tonumber(value)
                end
            end
        end
        results['name'] = queue
        return results
    else
        local queues = redis.call('zrange', Qsome.ns .. 'queues', 0, -1)
        local response = {}
        for index, qname in ipairs(queues) do
            table.insert(response, Qsome.queues(now, qname))
        end
        return response
    end
end

--! @brief Return information about the jobs in queues
function Qsome.job_states(now, queue)
    if queue then
        return Qsome.queue(queue):jobs(now)
    else
        local queues = Qsome.queues()
        local response = {}
        for index, qname in ipairs(queues) do
            table.insert(response, Qless.queues(now, qname))
        end
        return response
    end
end

function Qsome.tracked()
    local response = {
        jobs = {},
        expired = {}
    }
    local jids = redis.call('zrange', 'ql:tracked', 0, -1)
    for index, jid in ipairs(jids) do
        local data = Qsome.job(jid):data()
        if data then
            table.insert(response.jobs, data)
        else
            table.insert(response.expired, jid)
        end
    end
    return response
end

function Qsome.failed(group, start, limit)
    if group then
        local response = Qless.failed(group, start, limit)
        for i, jid in ipairs(response.jobs) do
            response.jobs[i] = Qsome.job(jid):data()
        end
        return response
    else
        return Qless.failed()
    end
end
