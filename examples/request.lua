#!/usr/bin/env lua
-- make sure zbusd is started before execution!
local zbus = require("zbus")
local cjson = require("cjson")

local m = zbus.member({serialize=cjson.encode, unserialize=cjson.decode})

local url = arg[1] or "matthias"
local params = arg[2] or '["mehr..."]'

local response = m:call(url, params)
print(response)