"""Lua scripts used by the Valkey/Redis backend."""

ADVANCE_LAST_TS = """
local current = tonumber(redis.call('HGET', KEYS[1], 'last_ts') or '0')
local new_ts = tonumber(ARGV[1])
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
local ids = redis.call('ZRANGEBYLEX', pending, minb, maxb, 'LIMIT', 0, limit * 16)
local out = {}
local moved = 0
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
if redis.call('ZCARD', pending) == 0 and redis.call('ZCARD', claimed) == 0 and redis.call('ZCARD', reserved) == 0 then
  redis.call('SREM', queues, queue)
end
return out
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
local ids = {}
if exact_id ~= '' then
  ids = {exact_id}
else
  ids = redis.call('ZRANGEBYLEX', source_pending, minb, maxb, 'LIMIT', 0, limit * 16)
end
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
if redis.call('ZCARD', source_pending) == 0 and redis.call('ZCARD', source_claimed) == 0 and redis.call('ZCARD', source_reserved) == 0 then
  redis.call('SREM', queues, source_queue)
end
return out
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
local ids = redis.call('ZRANGEBYLEX', pending, minb, maxb, 'LIMIT', 0, limit * 16)
local selected = {}
local out = {}
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
if #selected > 0 then
  redis.call('HSET', batch_meta, 'op', op, 'source', queue, 'target', target, 'created_ns', created_ns)
  redis.call('SADD', queues, queue)
end
return out
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
