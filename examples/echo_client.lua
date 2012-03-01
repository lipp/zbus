-- load zbus module
local zbus = require'zbus'

-- create a zbus member with the specified serializers
local member = zbus.member()
-- register a function, which will be called, when a zbus-message's url matches expression
member:call(
	'echo', -- the method url/name
	'Hello there' -- the argument string
)
