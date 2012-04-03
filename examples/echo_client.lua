local zm = require'zbus.member'
local member = zm.new()
-- call the service function
for i=1,arg[1] or 1 do
  local result_str = member:call(
    'echo', -- the method url/name
    'Hello there' -- the argument string
  )
  assert(result_str == 'Hello there')
end
