#!/usr/bin/env lua
-- make sure zbusd is started before execution!
local zbus = require'zbus'
local zconfig = require'zbus.json'

local member = zbus.member(zconfig)

-- simply subscribe notification given as parameter or all notfifications for printout
member:listen_add(arg[1] or '.*', print)

member:loop()
