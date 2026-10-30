-- All accessed keys are explicit. This deployment uses a single Redis instance.
-- KEYS: stock, buyers(set), stream, activity(hash), user->order(hash), request(hash).
local types={'string','set','stream','hash','hash','hash'}
for i=1,6 do
  local t=redis.call('TYPE',KEYS[i]).ok
  if t~='none' and t~=types[i] then return 4 end
end
local stock=tonumber(redis.call('GET',KEYS[1]))
local start=tonumber(redis.call('HGET',KEYS[4],'begin'))
local finish=tonumber(redis.call('HGET',KEYS[4],'end'))
if not stock or not start or not finish or redis.call('HGET',KEYS[4],'state')~='ACTIVE' then return 3 end
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if now<start or now>finish then return 3 end
-- Duplicate status remains queryable even after inventory is sold out.
if redis.call('SISMEMBER',KEYS[2],ARGV[2])==1 then return 2 end
if stock<=0 then return 1 end
redis.call('DECR',KEYS[1])
redis.call('SADD',KEYS[2],ARGV[2])
redis.call('HSET',KEYS[5],ARGV[2],ARGV[3])
redis.call('HSET',KEYS[6],'orderId',ARGV[3],'userId',ARGV[2],'voucherId',ARGV[1],'state','PROCESSING')
redis.call('EXPIRE',KEYS[6],604800)
redis.call('XADD',KEYS[3],'*','userId',ARGV[2],'voucherId',ARGV[1],'id',ARGV[3])
return 0