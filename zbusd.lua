#!/usr/bin/env lua
local zbus = require'zbus'
zbus.broker{
  reg_url = arg[1],
  log = function(...) print('zbusd',...) end
}:loop()
