local types={'string','set','hash','hash'}
for i=1,4 do
 local t=redis.call('TYPE',KEYS[i]).ok
 if t~='none' and t~=types[i] then return -2 end
end
local stock=redis.call('GET',KEYS[1])
if not stock or not tonumber(stock) then return -1 end
-- Only compensate the exact reservation, once; never release a later request.
local owner=redis.call('HGET',KEYS[3],ARGV[1])
if owner~=ARGV[2] then return 0 end
redis.call('HDEL',KEYS[3],ARGV[1])
redis.call('SREM',KEYS[2],ARGV[1])
redis.call('INCR',KEYS[1])
redis.call('HSET',KEYS[4],'state','FAILED')
return 1
