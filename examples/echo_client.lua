-- load zbus module
local zbus = require'zbus'

-- create a zbus member with the specified serializers
local member = zbus.member()

-- call the service function
local result_str = member:call(
	'echo', -- the method url/name
	'Hello there' -- the argument string
)
assert(result_str == 'Hello there')
