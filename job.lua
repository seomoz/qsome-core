-- Complete this job
function QsomeJob:complete(now, worker, queue, data, ...)
    -- We're ignoring the queue provided by the worker for now. At some point
    -- we may want to do something like verify that they're in the same super
    -- queue, but I think we're ok for now
    local rqueue = redis.call('hget', 'ql:j:' .. self.jid, 'queue')
    Qless.job(self.jid):complete(now, worker, rqueue, data, unpack(arg))
end

-- Return the data associated with this particular job
function QsomeJob:data()
    local data = Qless.job(self.jid):data()
    -- Now augment the job data with a hash property
    data['hash'] = redis.call('hget', 'ql:j:' .. self.jid, 'hash')
    return data
end

--! @brief Return the hash of the job
function QsomeJob:hash()
    return redis.call('hget', 'ql:j:' .. self.jid, 'hash')
end
