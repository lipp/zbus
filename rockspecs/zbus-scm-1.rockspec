package = 'zbus'
version = 'scm-1'
source = {
  url = 'git://github.com/lipp/zbus.git',
}
description = {
  summary = 'A zeromq based message distribution framework in Lua.',
  homepage = 'http://github.com/lipp/zbus',
  license = 'MIT/X11'
}
dependencies = {
  'lua >= 5.1',
  'lua-ev',
  'lua-zmq >= 1.0',
  'lua-cjson >= 1.0'
}
build = {
  type = 'none',
  install = {
    lua = {
      ["zbus"] = 'zbus.lua',
      ["zbus.json"] = 'zbus/json.lua'
    },
    bin = {
      'zbusd.lua'
    }
  }
}
