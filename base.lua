-- Qsome. Like qless, but a little more
local Qsome = {
    namespace = 'qs'
}

-- Forward declaration of QsomeQueue
local QsomeQueue = {}
QsomeQueue.__index = QsomeQueue

-- Forward declaration of QsomeJob
local QsomeJob = {}
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
