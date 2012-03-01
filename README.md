# About

zbus is a message bus in Lua. 

## Files

-    zbusd.lua: A Lua program, which acts as "message broker" or "router"
-    zbus.lua: The Lua module, which provides an API for being a zbus member (zbus.member)
-    zbus/json.lua: An optional Lua module, which defines JSON serialization methods

## Purpose

zbus is designed to allow:

-    inter-process method calls
-    inter-process notifications (publish/subscribe)

To achieve this, you have to become a zbus.member. zbus members can:

-  register callbacks to handle inter-process method calls.
-  register callbacks to handle notifications (publish/**subscribe**)
-  send notificiations (**publish**/subscribe)
-  call methods in another process
-  a extendable event loop

## Requirements

zbusd heavily relies on [lua-zmq](https://github.com/Neopallium/lua-zmq) and [lua-ev](https://github.com/brimworks/lua-ev). The optional JSON message wrapper (zbus/json.lua) requires [lua-cjson](http://www.kyne.com.au/~mark/software/lua-cjson.php). They are all available via luarocks and will be installed automatically with the zbus rock.

## Other Languages like C,Python,...

Even if the broker (zbusd.lua) and the modules provided are written in Lua, zbus members could be written in **any language** with support for zeromq (and multi-part messages), as [lua-zmq](https://github.com/Neopallium/lua-zmq) does.

## Protocol

zbus defines a simple protocol based on zeromq **multi-part messages**.This allows zbusd.lua to effectively recognize (or simply forward):

-    method-urls
-    method-arguments
-    return-values
-    exceptions
-    notification-data

The zbus protocol itself is aware of any dataformat but provides a default implementation for JSON which allows a very convient zbus.

## Build

zbus is Lua-only, so no build/compile process is involved.

## Install

Latest version from github:

       $ sudo luarocks install https://github.com/lipp/zbus/raw/master/rockspecs/zbus-scm-1.rockspec


or from cloned repo directory:

   $ sudo luarocks make rockspecs/zbus-scm-1.rockspec

There is no official release yet.

# Example

## zbusd.lua

**All examples require zbusd to run**:

      $ zbusd.lua


It is a daemon process and will never return. If the zbusd.lua daemon is not started, all zbus.members will block until zbusd.lua is started.

## Providing an echo service and client without argument serialization

### The server providing the 'echo' method

```lua
-- load zbus module
local zbus = require'zbus'

-- create a default zbus member
local member = zbus.member()

-- register a function, which will be called, when a zbus-message's url matches expression
member:replier_add(
	 -- the expression to match	
          '^echo$', 
	  -- the callback gets passed in the matched url, in this case always 'echo', and the unserialized argument string	
          function(url,argument_str) 
		print(url,argument_str)
		return argument_str
          end)

-- start the event loop, which will forward all 'echo' calls to member.
member:loop()

```

### The client calling the 'echo' method

```lua
-- load zbus module
local zbus = require'zbus'

-- create a default zbus member
local member = zbus.member()

-- call the service function and pass some argument string
local result_str = member:call(
	'echo', -- the method url/name
	'Hello there' -- the argument string
)
-- verify that the echo service works
assert(result_str=='Hello there')
```

### Run the example
check is zbusd.lua is running! The echo_server.lua will never return (it is a service!) and must be terminated with aisgnal of choice, e.g. kill.

      $ lua examples/echo_server.lua &
      $ lua examples/echo_client


## Providing an echo service and client with JSON serialization

If a serialization config is provided, we can work with multiple typed arguments and return values.
What the zbus_json_config does, is wrapping/unwrapping the arguments and results to a JSON array.

### The server providing the 'echo' method

```lua
-- load zbus module
local zbus = require'zbus'

-- load the JSON message format serilization
local zbus_json_config = require'zbus.json'

-- create a zbus member with the specified serializers
local member = zbus.member(zbus_json_config)

-- register a function, which will be called, when a zbus-message's url matches expression
member:replier_add(
	 -- the expression to match	
          '^echo$', 
	  -- the callback gets passed in the matched url, in this case always 'echo', and the unserialized argument string	
          function(url,...) 
		print(url,...)
		return ...
          end)

-- start the event loop, which will forward all 'echo' calls to member.
member:loop()

```

### The client calling the 'echo' method

```lua
-- load zbus module
local zbus = require'zbus'

-- load the JSON message format serilization
local zbus_json_config = require'zbus.json'

-- create a zbus member with the specified serializers
local member = zbus.member(zbus_json_config)

-- call the service function and pass some arguments
local res = {member:call(
	'echo', -- the method url/name
	'Hello',123,'is my number',{stuff=8181} -- the arguments
)}
-- verify that the echo service works
assert(res[1]=='Hello')
assert(res[2]==123)
assert(res[3]=='is my number')
assert(res[4].stuff==8181)
```

### Run the example
check is zbusd.lua is running! The echo_server.lua will never return (it is a service!) and must be terminated with aisgnal of choice, e.g. kill.

      $ lua examples/echo_server_json.lua &
      $ lua examples/echo_client_json.lua




