-- load zbus module
local zbus = require'zbus'

-- load the JSON message format serilization
local zbus_json_config = require'zbus.json'

-- create a zbus member with the specified serializers
local member = zbus.member(zbus_json_config)
-- register a function, which will be called, when a zbus-message's url matches expression
member:call(
	'echo', -- the method url/name
	'Hello',123,'is my number',{stuff=8181} -- the arguments
)
