#!/usr/bin/env lua
local zbus = require'zbus'
local broker = zbus.broker.new{
  reg_url = arg[1],
  log = function(...) print('zbusd',...) end
}
broker:loop()
