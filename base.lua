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
function Qsome.queues()
    return redis.call('zrange', Qsome.ns .. 'queues', 0, -1)
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
