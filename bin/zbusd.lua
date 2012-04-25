#!/usr/bin/env lua
local broker = require'zbus.broker'.new{
  reg_url = arg[1],
  debug = true,
  log = function(...) print('zbusd',...) end
}
broker:loop()
