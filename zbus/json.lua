local cjson = require'cjson'
local unpack = unpack
local assert = assert
local zbus = require'zbus'
local pcall = pcall
local type = type
local tostring = tostring

module('zbus.json')

local encode = cjson.encode
local decode = cjson.decode

serialize_array = 
  function(...)
    return encode{...}
  end

unserialize_array = 
  function(s)
    return unpack(decode(s))
  end

serialize_args = serialize_array
unserialize_args = unserialize_array

serialize_result = serialize_array
unserialize_result = unserialize_array

serialize_err = 
  function(err)
    if type(err) == 'table' then
      assert(err.code and err.message)
      return encode(err)
    else
      local _,msg = pcall(tostring,err) 
      return encode{
        code = -32603,
        message = msg or 'Unknown Error'
      }
    end
  end

serialize_zerr = 
  function(code,msg)
    return encode{
      message = 'Method not found',
      code = -32601
    }
  end

unserialize_err = 
  function(err)
    return decode(err)
  end

make_zerr = 
  function(code,msg)
    return {
      code = -32603,
      message = 'Internal error',
      data = {
        zerr = {
          code = code,
          message = msg
        }
      }
    }
  end

