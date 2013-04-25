-------------------------------------------------------------------------------
-- QsomeQueue Definition
-------------------------------------------------------------------------------

--! @brief Create a queue with the provided number of subqueues
--! @param subqueues - the number of subqueues in this queue
function QsomeQueue:create(subqueues)
end

--! @brief Destroy the queue
function QsomeQueue:destroy()
    error('Destroy(): Method intentionally unimplemented')
end

--! @brief Pop the provided number of jobs from the queue
--! @param count - number of jobs to pop
function QsomeQueue:pop(count)
    error('Pop(): Method umimplemented')
end

--! @brief Enqueue a job to be executed
--! @param now - the current timestamp in seconds since epoch
--! @param jid - job id
--! @param klass - job's class name
--! @param data - json-encoded data for the job
--! @param hash - integer hash associated with the job
--! @param delay - seconds the job must wait before running
--! @param priority - integral job priority
function QsomeQueue:put(now, jid, klass, data, hash, delay, priority)    
end

--! @brief Change the number of subqueues in this queue
--! @param size - new number of subqueues
function QsomeQueue:resize(size)
end

--! @brief List the subqueues of the provided queue
function QsomeQueue:subqueues()
    return {}
end
