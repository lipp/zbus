local ev = require'ev'
local socket = require'socket'
require'pack'

local print = print
local pairs = pairs
local tinsert = table.insert
local ipairs = ipairs
local assert = assert
local spack = string.pack
local log = print

module('zbus.socket')

local receive_msg = 
   function(self)
      local parts = {}
      while true do
         local _,bytes = self:receive(4):unpack('>I')
         if bytes == 0 then 
            break
         end
         local part = self:receive(bytes)
         tinsert(parts,part)
      end
      return parts
   end

local send_msg = 
   function(self,parts)
      local msg = ''
      for i,part in ipairs(parts) do
         msg = msg..spack('>I',#part)..part
      end
      msg = msg..spack('>I',0)      
      self:send(msg)
   end

local wrap = 
   function(sock)
      sock:settimeout(0)
      sock:setoption('tcp-nodelay',true)
      local on_message = function() end
      local on_close = function() end
      local wrapped = {}
      wrapped.send_msg =                              
         function(_,parts)
            local msg = ''
            for i,part in ipairs(parts) do
               msg = msg..spack('>I',#part)..part
            end
            msg = msg..spack('>I',0)
            local len = #msg
            local pos = 1
            ev.IO.new(
               function(loop,write_io)                                
                  while pos < len do
                     local err                                    
                     pos,err = sock:send(msg,pos)
                     if not pos then
                        if err == 'timeout' then
                           return
                        elseif err == 'closed' then
                           write_io:stop(loop)
                           sock:shutdown()
                           sock:close()
                           return
                        end
                     end
                  end
                  write_io:stop(loop)
               end,
               sock:getfd(),
               ev.WRITE
            ):start(ev.Loop.default)
         end
      wrapped.on_close = 
         function(_,f)
            on_close = f
         end
      wrapped.on_message = 
         function(_,f)
            on_message = f
         end     
      wrapped.read_io = 
         function()
            local parts = {}
            local part
            local left
            local length
            local header
            local _
            
            return ev.IO.new(
               function(loop,read_io)
                  while true do
                     if not header or #header < 4 then
                        local err,sub 
                        header,err,sub = sock:receive(4,header)
                        if err then
                           if err == 'timeout' then
                              header = sub
                              return                                    
                           else
                              if err ~= 'closed' then
                                 log('ERROR','unknown socket error',err)
                              end
                              read_io:stop(loop)
                              sock:shutdown()
                              sock:close()
                              on_close(wrapped)
                              return                           
                           end
                        end
                        if #header == 4 then
                           _,left = header:unpack('>I')
                           if left == 0 then
                              on_message(parts,wrapped)
                              parts = {}
                              part = nil
                              left = nil
                              length = nil
                              header = nil
                           else
                              length = left
                           end
                        end
                     end
                     if length then
                        if not part or #part ~= length then
                           local err,sub
                           part,err,sub = sock:receive(length,part)
                           if err then
                              if err == 'timeout' then
                                 part = sub
                                 left = length - #part
                                 return
                              else 
                                 if err ~= 'closed' then
                                    log('ERROR','unknown socket error',err)
                                 end
                                 read_io:stop(loop)
                                 sock:shutdown()
                                 sock:close()
                                 on_close(wrapped)                                
                                 return
                              end
                           end
                           if #part == length then
                              tinsert(parts,part)
                              part = nil
                              left = nil
                              length = nil
                              header = nil
                           end
                        end
                     end -- if length
                  end -- while
               end,
         sock:getfd(),
         ev.READ)
         end
      return wrapped
   end



local listener = 
   function(port,on_connect)
      local sock = assert(socket.bind('*',port))
      sock:settimeout(0)
      local listen_io = ev.IO.new(
         function(loop,accept_io)
            local client = assert(sock:accept())         
            local wrapped = wrap(client)
            wrapped:read_io():start(loop)
            on_connect(wrapped)
         end,
         sock:getfd(),
         ev.READ)
      return {
         io = listen_io
      }
   end

return {
   listener = listener,
   wrap = wrap,
   send_msg = send_msg,
   receive_msg = receive_msg
}

