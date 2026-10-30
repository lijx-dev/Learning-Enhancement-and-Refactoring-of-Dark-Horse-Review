-- Redis 7 reports undelivered entries separately from entries already in the PEL.
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
for _, group in ipairs(groups) do
    local name, lag
    for i = 1, #group, 2 do
        if group[i] == 'name' then name = group[i + 1] end
        if group[i] == 'lag' then lag = group[i + 1] end
    end
    if name == ARGV[1] then return tonumber(lag) or -1 end
end
return -1
