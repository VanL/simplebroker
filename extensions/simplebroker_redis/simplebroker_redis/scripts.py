"""Lua scripts used by the Valkey/Redis backend."""

ADVANCE_LAST_TS = """
local function pad19(value)
  local text = tostring(value or '0')
  return string.rep('0', 19 - string.len(text)) .. text
end
local current = pad19(redis.call('HGET', KEYS[1], 'last_ts') or '0')
local new_ts = ARGV[2]
if current < new_ts then
  redis.call('HSET', KEYS[1], 'last_ts', ARGV[1])
  return 1
end
return 0
"""

WRITE_MESSAGE = """
local meta = KEYS[1]
local bodies = KEYS[2]
local all_ids = KEYS[3]
local pending = KEYS[4]
local queues = KEYS[5]
local queue = ARGV[1]
local id = ARGV[2]
local body = ARGV[3]
if redis.call('HGET', meta, 'magic') == false then
  return {-2}
end
if redis.call('HEXISTS', bodies, id) == 1 or redis.call('ZSCORE', all_ids, id) ~= false then
  return {-1}
end
redis.call('HSET', bodies, id, body)
redis.call('ZADD', all_ids, 0, id)
redis.call('ZADD', pending, 0, id)
redis.call('SADD', queues, queue)
return {1}
"""

INSERT_MESSAGES = """
local meta = KEYS[1]
local bodies = KEYS[2]
local all_ids = KEYS[3]
local queues = KEYS[4]
local required_last_ts = ARGV[2]
local count = tonumber(ARGV[3])
if redis.call('HGET', meta, 'magic') == false then
  return {-2}
end
local seen = {}
for index = 1, count do
  local arg_offset = 4 + ((index - 1) * 3)
  local id = ARGV[arg_offset + 1]
  if seen[id] == true then
    return {-3}
  end
  seen[id] = true
  if redis.call('HEXISTS', bodies, id) == 1 or redis.call('ZSCORE', all_ids, id) ~= false then
    return {-1}
  end
end
local function pad19(value)
  local text = tostring(value or '0')
  return string.rep('0', 19 - string.len(text)) .. text
end
local current = pad19(redis.call('HGET', meta, 'last_ts') or '0')
if current < required_last_ts then
  redis.call('HSET', meta, 'last_ts', ARGV[1])
end
for index = 1, count do
  local key_offset = 4 + index
  local arg_offset = 4 + ((index - 1) * 3)
  local pending = KEYS[key_offset]
  local queue = ARGV[arg_offset]
  local id = ARGV[arg_offset + 1]
  local body = ARGV[arg_offset + 2]
  redis.call('HSET', bodies, id, body)
  redis.call('ZADD', all_ids, 0, id)
  redis.call('ZADD', pending, 0, id)
  redis.call('SADD', queues, queue)
end
return {1}
"""

BROADCAST_MESSAGE = """
local meta = KEYS[1]
local bodies = KEYS[2]
local all_ids = KEYS[3]
local queues = KEYS[4]
local required_last_ts = ARGV[2]
local capacity = tonumber(ARGV[3])
local body = ARGV[4]
local pending_prefix = ARGV[5]
if redis.call('HGET', meta, 'magic') == false then
  return {-2}
end
local queue_names = redis.call('SMEMBERS', queues)
table.sort(queue_names)
if #queue_names > capacity then
  return {-4, #queue_names}
end
local seen = {}
for index = 1, #queue_names do
  local id = ARGV[5 + index]
  if seen[id] == true then
    return {-3}
  end
  seen[id] = true
  if redis.call('HEXISTS', bodies, id) == 1 or redis.call('ZSCORE', all_ids, id) ~= false then
    return {-1}
  end
end
local function pad19(value)
  local text = tostring(value or '0')
  return string.rep('0', 19 - string.len(text)) .. text
end
local current = pad19(redis.call('HGET', meta, 'last_ts') or '0')
if current < required_last_ts then
  redis.call('HSET', meta, 'last_ts', ARGV[1])
end
local response = {1}
for index, queue in ipairs(queue_names) do
  local id = ARGV[5 + index]
  redis.call('HSET', bodies, id, body)
  redis.call('ZADD', all_ids, 0, id)
  redis.call('ZADD', pending_prefix .. queue .. ':pending', 0, id)
  table.insert(response, queue)
end
return response
"""

CLAIM_MESSAGES = """
local pending = KEYS[1]
local claimed = KEYS[2]
local reserved = KEYS[3]
local bodies = KEYS[4]
local queues = KEYS[5]
local queue = ARGV[1]
local limit = tonumber(ARGV[2])
local minb = ARGV[3]
local maxb = ARGV[4]
local out = {}
local moved = 0
local cursor = ''
local scan_minb = minb
local window_size = limit * 16
-- Each invocation examines at most 16 windows (256 * limit candidates), so
-- reserved prefixes cannot create an unbounded Lua event-loop stall. The
-- caller resumes after cursor. A concurrently released id behind cursor is
-- intentionally left for the next public operation.
for window = 1, 16 do
  local ids = redis.call('ZRANGEBYLEX', pending, scan_minb, maxb, 'LIMIT', 0, window_size)
  if #ids == 0 then
    cursor = ''
    break
  end
  cursor = ids[#ids]
  for _, id in ipairs(ids) do
    if redis.call('ZSCORE', reserved, id) == false then
      local body = redis.call('HGET', bodies, id)
      if body ~= false then
        redis.call('ZREM', pending, id)
        redis.call('ZADD', claimed, 0, id)
        table.insert(out, body)
        table.insert(out, id)
        moved = moved + 1
        if moved >= limit then break end
      else
        redis.call('ZREM', pending, id)
      end
    end
  end
  if moved > 0 or #ids < window_size then
    cursor = ''
    break
  end
  scan_minb = '(' .. cursor
end
if redis.call('ZCARD', pending) == 0 and redis.call('ZCARD', claimed) == 0 and redis.call('ZCARD', reserved) == 0 then
  redis.call('SREM', queues, queue)
end
local response = {cursor}
for _, value in ipairs(out) do table.insert(response, value) end
return response
"""

MOVE_MESSAGES = """
local source_pending = KEYS[1]
local source_claimed = KEYS[2]
local source_reserved = KEYS[3]
local target_pending = KEYS[4]
local bodies = KEYS[5]
local queues = KEYS[6]
local source_queue = ARGV[1]
local target_queue = ARGV[2]
local limit = tonumber(ARGV[3])
local minb = ARGV[4]
local maxb = ARGV[5]
local exact_id = ARGV[6]
local require_unclaimed = ARGV[7] == '1'
local out = {}
local moved = 0
local cursor = ''
local scan_minb = minb
local window_size = limit * 16
-- Bounded to 16 windows per invocation; Python resumes after cursor. A
-- concurrently released id behind cursor waits for the next public operation.
for window = 1, 16 do
  local ids = {}
  if exact_id ~= '' then
    ids = {exact_id}
  else
    ids = redis.call('ZRANGEBYLEX', source_pending, scan_minb, maxb, 'LIMIT', 0, window_size)
  end
  if #ids == 0 then
    cursor = ''
    break
  end
  cursor = ids[#ids]
  for _, id in ipairs(ids) do
    local from_pending = redis.call('ZSCORE', source_pending, id) ~= false
    local from_claimed = redis.call('ZSCORE', source_claimed, id) ~= false
    local is_reserved = redis.call('ZSCORE', source_reserved, id) ~= false
    if (not is_reserved) and (from_pending or ((not require_unclaimed) and from_claimed)) then
      local body = redis.call('HGET', bodies, id)
      if body ~= false then
        redis.call('ZREM', source_pending, id)
        redis.call('ZREM', source_claimed, id)
        redis.call('ZADD', target_pending, 0, id)
        redis.call('SADD', queues, target_queue)
        table.insert(out, body)
        table.insert(out, id)
        moved = moved + 1
        if moved >= limit then break end
      end
    end
  end
  if moved > 0 or exact_id ~= '' or #ids < window_size then
    cursor = ''
    break
  end
  scan_minb = '(' .. cursor
end
if redis.call('ZCARD', source_pending) == 0 and redis.call('ZCARD', source_claimed) == 0 and redis.call('ZCARD', source_reserved) == 0 then
  redis.call('SREM', queues, source_queue)
end
local response = {cursor}
for _, value in ipairs(out) do table.insert(response, value) end
return response
"""

DELETE_MESSAGE_IDS = """
local pending = KEYS[1]
local claimed = KEYS[2]
local reserved = KEYS[3]
local bodies = KEYS[4]
local all_ids = KEYS[5]
local queues = KEYS[6]
local queue = ARGV[1]
for i = 2, #ARGV do
  if redis.call('ZSCORE', reserved, ARGV[i]) ~= false then
    return -1
  end
end
local deleted = 0
for i = 2, #ARGV do
  local id = ARGV[i]
  local removed_pending = redis.call('ZREM', pending, id)
  local removed_claimed = redis.call('ZREM', claimed, id)
  if removed_pending > 0 or removed_claimed > 0 then
    redis.call('HDEL', bodies, id)
    redis.call('ZREM', all_ids, id)
    deleted = deleted + 1
  end
end
if redis.call('ZCARD', pending) == 0 and redis.call('ZCARD', claimed) == 0 and redis.call('ZCARD', reserved) == 0 then
  redis.call('SREM', queues, queue)
end
return deleted
"""

DELETE_QUEUE = """
local pending = KEYS[1]
local claimed = KEYS[2]
local reserved = KEYS[3]
local bodies = KEYS[4]
local all_ids = KEYS[5]
local queues = KEYS[6]
local queue = ARGV[1]
if redis.call('ZCARD', reserved) > 0 then
  return -1
end
local pending_ids = redis.call('ZRANGE', pending, 0, -1)
local claimed_ids = redis.call('ZRANGE', claimed, 0, -1)
local seen = {}
local deleted = 0
local function delete_ids(ids)
  for _, id in ipairs(ids) do
    if seen[id] == nil then
      redis.call('HDEL', bodies, id)
      redis.call('ZREM', all_ids, id)
      seen[id] = true
      deleted = deleted + 1
    end
  end
end
delete_ids(pending_ids)
delete_ids(claimed_ids)
redis.call('DEL', pending, claimed, reserved)
redis.call('SREM', queues, queue)
return deleted
"""

VACUUM_CLAIMED = """
local claimed = KEYS[1]
local pending = KEYS[2]
local reserved = KEYS[3]
local bodies = KEYS[4]
local all_ids = KEYS[5]
local queues = KEYS[6]
local queue = ARGV[1]
local limit = tonumber(ARGV[2])
local ids = redis.call('ZRANGE', claimed, 0, limit - 1)
for _, id in ipairs(ids) do
  redis.call('ZREM', claimed, id)
  redis.call('HDEL', bodies, id)
  redis.call('ZREM', all_ids, id)
end
if redis.call('ZCARD', pending) == 0 and redis.call('ZCARD', claimed) == 0 and redis.call('ZCARD', reserved) == 0 then
  redis.call('SREM', queues, queue)
end
return #ids
"""

DELETE_FROM_QUEUES = """
local bodies = KEYS[1]
local all_ids = KEYS[2]
local queues = KEYS[3]
local queue_count = tonumber(ARGV[1])
local maxb = ARGV[2]

local function pending_key(index)
  return KEYS[3 + ((index - 1) * 3) + 1]
end

local function claimed_key(index)
  return KEYS[3 + ((index - 1) * 3) + 2]
end

local function reserved_key(index)
  return KEYS[3 + ((index - 1) * 3) + 3]
end

for i = 1, queue_count do
  local reserved_matches = redis.call('ZRANGEBYLEX', reserved_key(i), '-', maxb, 'LIMIT', 0, 1)
  if #reserved_matches > 0 then
    return -1
  end
end

local deleted = 0
local seen = {}

local function delete_ids(source, ids)
  for _, id in ipairs(ids) do
    local removed = redis.call('ZREM', source, id)
    if removed > 0 and seen[id] == nil then
      redis.call('HDEL', bodies, id)
      redis.call('ZREM', all_ids, id)
      seen[id] = true
      deleted = deleted + 1
    end
  end
end

for i = 1, queue_count do
  local pending = pending_key(i)
  local claimed = claimed_key(i)
  local reserved = reserved_key(i)
  local queue = ARGV[2 + i]
  local pending_ids = redis.call('ZRANGEBYLEX', pending, '-', maxb)
  local claimed_ids = redis.call('ZRANGEBYLEX', claimed, '-', maxb)

  delete_ids(pending, pending_ids)
  delete_ids(claimed, claimed_ids)

  if redis.call('ZCARD', pending) == 0 and redis.call('ZCARD', claimed) == 0 and redis.call('ZCARD', reserved) == 0 then
    redis.call('SREM', queues, queue)
  end
end

return deleted
"""

RENAME_QUEUE = """
local old_pending = KEYS[1]
local old_claimed = KEYS[2]
local old_reserved = KEYS[3]
local new_pending = KEYS[4]
local new_claimed = KEYS[5]
local new_reserved = KEYS[6]
local queues = KEYS[7]
local aliases = KEYS[8]
local meta = KEYS[9]
local old_queue = ARGV[1]
local new_queue = ARGV[2]
local retarget_aliases = ARGV[3] == '1'
local alias_version = ARGV[4]

if redis.call('ZCARD', old_reserved) > 0 then
  return {-1, 0, 0}
end
if redis.call('ZCARD', new_pending) > 0 or redis.call('ZCARD', new_claimed) > 0 or redis.call('ZCARD', new_reserved) > 0 then
  return {-2, 0, 0}
end

local pending_count = redis.call('ZCARD', old_pending)
local claimed_count = redis.call('ZCARD', old_claimed)
local messages_renamed = pending_count + claimed_count
if messages_renamed == 0 then
  return {1, 0, 0}
end

if pending_count > 0 then
  redis.call('RENAME', old_pending, new_pending)
else
  redis.call('DEL', old_pending)
end
if claimed_count > 0 then
  redis.call('RENAME', old_claimed, new_claimed)
else
  redis.call('DEL', old_claimed)
end
redis.call('DEL', old_reserved)
redis.call('SREM', queues, old_queue)
redis.call('SADD', queues, new_queue)

local aliases_retargeted = 0
if retarget_aliases then
  local alias_items = redis.call('HGETALL', aliases)
  for index = 1, #alias_items, 2 do
    if alias_items[index + 1] == old_queue then
      redis.call('HSET', aliases, alias_items[index], new_queue)
      aliases_retargeted = aliases_retargeted + 1
    end
  end
  if aliases_retargeted > 0 then
    redis.call('HSET', meta, 'alias_version', alias_version)
  end
end

return {1, messages_renamed, aliases_retargeted}
"""

BEGIN_BATCH = """
local pending = KEYS[1]
local reserved = KEYS[2]
local bodies = KEYS[3]
local batch_ids = KEYS[4]
local batch_meta = KEYS[5]
local queues = KEYS[6]
local queue = ARGV[1]
local token = ARGV[2]
local op = ARGV[3]
local target = ARGV[4]
local created_ns = ARGV[5]
local limit = tonumber(ARGV[6])
local minb = ARGV[7]
local maxb = ARGV[8]
local selected = {}
local out = {}
local cursor = ''
local scan_minb = minb
local window_size = limit * 16
-- Bounded to 16 windows per invocation; Python resumes after cursor. A
-- concurrently released id behind cursor waits for the next public operation.
for window = 1, 16 do
  local ids = redis.call('ZRANGEBYLEX', pending, scan_minb, maxb, 'LIMIT', 0, window_size)
  if #ids == 0 then
    cursor = ''
    break
  end
  cursor = ids[#ids]
  for _, id in ipairs(ids) do
    if redis.call('ZSCORE', reserved, id) == false then
      local body = redis.call('HGET', bodies, id)
      if body ~= false then
        redis.call('ZADD', reserved, 0, id)
        redis.call('SADD', batch_ids, id)
        table.insert(selected, id)
        table.insert(out, body)
        table.insert(out, id)
        if #selected >= limit then break end
      end
    end
  end
  if #selected > 0 or #ids < window_size then
    cursor = ''
    break
  end
  scan_minb = '(' .. cursor
end
if #selected > 0 then
  redis.call('HSET', batch_meta, 'op', op, 'source', queue, 'target', target, 'created_ns', created_ns)
  redis.call('SADD', queues, queue)
end
local response = {cursor}
for _, value in ipairs(out) do table.insert(response, value) end
return response
"""

COMMIT_CLAIM_BATCH = """
local pending = KEYS[1]
local claimed = KEYS[2]
local reserved = KEYS[3]
local batch_ids = KEYS[4]
local batch_meta = KEYS[5]
local queues = KEYS[6]
local queue = ARGV[1]
if redis.call('HGET', batch_meta, 'op') ~= 'claim' or redis.call('HGET', batch_meta, 'source') ~= queue then
  return -1
end
local ids = redis.call('SMEMBERS', batch_ids)
for _, id in ipairs(ids) do
  redis.call('ZREM', pending, id)
  redis.call('ZREM', reserved, id)
  redis.call('ZADD', claimed, 0, id)
end
redis.call('DEL', batch_ids, batch_meta)
if redis.call('ZCARD', pending) == 0 and redis.call('ZCARD', claimed) == 0 and redis.call('ZCARD', reserved) == 0 then
  redis.call('SREM', queues, queue)
end
return #ids
"""

COMMIT_MOVE_BATCH = """
local source_pending = KEYS[1]
local source_reserved = KEYS[2]
local target_pending = KEYS[3]
local batch_ids = KEYS[4]
local batch_meta = KEYS[5]
local queues = KEYS[6]
local source_claimed = KEYS[7]
local source_queue = ARGV[1]
local target_queue = ARGV[2]
if redis.call('HGET', batch_meta, 'op') ~= 'move' or redis.call('HGET', batch_meta, 'source') ~= source_queue or redis.call('HGET', batch_meta, 'target') ~= target_queue then
  return -1
end
local ids = redis.call('SMEMBERS', batch_ids)
for _, id in ipairs(ids) do
  redis.call('ZREM', source_pending, id)
  redis.call('ZREM', source_reserved, id)
  redis.call('ZADD', target_pending, 0, id)
end
redis.call('SADD', queues, target_queue)
redis.call('DEL', batch_ids, batch_meta)
if redis.call('ZCARD', source_pending) == 0 and redis.call('ZCARD', source_claimed) == 0 and redis.call('ZCARD', source_reserved) == 0 then
  redis.call('SREM', queues, source_queue)
end
return #ids
"""

ROLLBACK_BATCH = """
local reserved = KEYS[1]
local batch_ids = KEYS[2]
local batch_meta = KEYS[3]
local ids = redis.call('SMEMBERS', batch_ids)
for _, id in ipairs(ids) do
  redis.call('ZREM', reserved, id)
end
redis.call('DEL', batch_ids, batch_meta)
return #ids
"""
