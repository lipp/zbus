#!/usr/bin/env lua
-- make sure zbusd is started before execution!
local zbus = require("zbus")
local zconfig = require("zbus.json")

local m = zbus.member(zconfig)
m:notify(arg[1] or "hallo", 123,{horst='doof'})
