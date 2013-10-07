-------------------------------------------------------------------------------
-- This is the analog of the 'main' function when invoking qsome directly, as
-- apposed to for use within another library
-------------------------------------------------------------------------------
local QsomeAPI = {}

-------------------------------------------------------------------------------
-- Top-level functions
-------------------------------------------------------------------------------
QsomeAPI.workers = function(now, worker)
    return cjson.encode(Qless.workers(now, worker))
end

QsomeAPI.failed = function(now, group, start, limit)
    return cjson.encode(Qsome.failed(group, start, limit))
end

-- Get information about a queue or queues
QsomeAPI.queues = function(now, queue)
    return cjson.encode(Qsome.queues(now, queue))
end

QsomeAPI.jobs = function(now, state, ...)
    return Qless.jobs(now, state, unpack(arg))
end

QsomeAPI.tracked = function(now)
    return cjson.encode(Qsome.tracked())
end

-------------------------------------------------------------------------------
-- Queue manipulation
-------------------------------------------------------------------------------
QsomeAPI['queue.subqueues'] = function(now, queue)
    return cjson.encode(Qsome.queue(queue):subqueues())
end

QsomeAPI['queue.resize'] = function(now, queue, size)
    return Qsome.queue(queue):resize(size)
end

QsomeAPI['put'] = function(
    now, queue, jid, klass, hash, data, delay, ...)
    return Qsome.queue(queue):put(
        now, jid, klass, hash, data, delay, unpack(arg))
end

QsomeAPI['pop'] = function(now, queue, worker, count)
    local jids = Qsome.queue(queue):pop(now, worker, count)
    local response = {}
    for i, jid in ipairs(jids) do
        table.insert(response, Qsome.job(jid):data())
    end
    return cjson.encode(response)
end

QsomeAPI['queue.config'] = function(now, queue, option, value)
    return cjson.encode(Qsome.queue(queue):config(option, value))
end

QsomeAPI['length'] = function(now, queue)
    return Qsome.queue(queue):length()
end

QsomeAPI['peek'] = function(now, queue, count)
    local jids = Qsome.queue(queue):peek(now, count)
    local response = {}
    for i, jid in ipairs(jids) do
        table.insert(response, Qsome.job(jid):data())
    end
    return cjson.encode(response)
end

QsomeAPI['stats'] = function(now, queue, date)
    return cjson.encode(Qsome.queue(queue):stats(now, date))
end

-------------------------------------------------------------------------------
-- Job manipulation
-------------------------------------------------------------------------------
-- Return json for the job identified by the provided jid. If the job is not
-- present, then `nil` is returned
function QsomeAPI.get(now, jid)
    local data = Qsome.job(jid):data()
    if not data then
        return nil
    end
    return cjson.encode(data)
end

-- Get a number of jobs
function QsomeAPI.multiget(now, ...)
  local results = {}
  for i, jid in ipairs(arg) do
    table.insert(results, Qsome.job(jid):data())
  end
  return cjson.encode(results)
end

QsomeAPI.complete = function(now, jid, worker, queue, data, ...)
    return Qsome.job(jid):complete(now, worker, queue, data, unpack(arg))
end

QsomeAPI.fail = function(now, jid, worker, group, message, data)
    return Qless.job(jid):fail(now, worker, group, message, data)
end

QsomeAPI.cancel = function(now, ...)
    return Qless.cancel(unpack(arg))
end

QsomeAPI['heartbeat'] = function(now, jid, worker, data)
    return Qsome.job(jid):heartbeat(now, worker, data)
end

QsomeAPI['track'] = function(now, command, jid)
    return cjson.encode(Qless.track(now, command, jid))
end

-------------------------------------------------------------------------------
-- Recurring Jobs
-------------------------------------------------------------------------------
QsomeAPI['recur.get'] = function(now, jid)
    return cjson.encode(Qless.recurring(jid):data())
end

-------------------------------------------------------------------------------
-- Configuration
-------------------------------------------------------------------------------
QsomeAPI['config.get'] = function(now, key)
    return cjson.encode(Qless.config.get(key))
end

QsomeAPI['config.set'] = function(now, key, value)
    return Qless.config.set(key, value)
end

-- Unset a configuration option
QsomeAPI['config.unset'] = function(now, key)
    return Qless.config.unset(key)
end

-------------------------------------------------------------------------------
-- Function lookup
-------------------------------------------------------------------------------

-- None of the qless function calls accept keys
if #KEYS > 0 then erorr('No Keys should be provided') end

-- The first argument must be the function that we intend to call, and it must
-- exist
local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
    QsomeAPI[command_name], 'Unknown command ' .. command_name)

-- The second argument should be the current time from the requesting client
local now          = tonumber(table.remove(ARGV, 1))
local now          = assert(
    now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))
