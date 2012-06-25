local ev = require'ev'
local socket = require'socket'
require'pack'

local print = print
local pairs = pairs
local tinsert = table.insert
local ipairs = ipairs
local assert = assert
local spack = string.pack

module('zbus.socket')

local receive_msg = 
   function(self)
      local parts = {}
      while true do
         local bytes = self:receive(4):unpack('>I')
         if bytes == 0 then 
            break
         end
         table.insert(parts,self:receive(bytes))
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

--local replier

--local xyz = 


local listener = 
   function(port,on_connect)
      local sock = assert(socket.bind('*',port))
      sock:settimeout(0)
      local listen_io = ev.IO.new(
         function(loop,accept_io)
            local client = assert(sock:accept())
            client:settimeout(0)
            client:setoption('tcp-nodelay',true)
            local on_message
            local on_close
            if on_connect then
               on_connect(
                  {
                     send_msg =                              
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
                                    pos,err = client:send(msg,pos)
                                    if not pos then
                                       if err == 'timeout' then
                                          return
                                       elseif err == 'closed' then
                                          write_io:stop(loop)
                                          client:shutdown()
                                          client:close()
                                          on_close()
                                          return
                                       end
                                    end
                                 end
                                 write_io:stop(loop)
                              end,
                              client:getfd(),
                              ev.WRITE
                           ):start(loop)
                        end,
                     on_close = 
                        function(_,f)
                           on_close = f
                        end,
                     on_message = 
                        function(_,f)
                           on_message = f
                        end
                  }
               )
            end
            local parts = {}
            local part
            local left
            local length
            local header
            local close = 
               function()
                  client:shutdown()
                  client:close()
                  on_close()
               end
--            local i = 0
            ev.IO.new(
               function(loop,read_io)
                  while true do
--                     print(i)
--                     i = i +1
                     if not header or #header < 4 then
                        local header_left = 4                              
                        if header then
                           header_left = 4-#header
                        else
                           header = ''
                        end                              
                        local header_more,err,sub = client:receive(header_left)
                        if err then
--                           print('err',err)
                           if err == 'timeout' then
                              if header then
                                 header = header..sub
                              else
                                 header = sub
                              end
                              return                                    
                           else -- if err == 'closed' then
                              close()
                              read_io:stop(loop)
                              return
                           end
                        end
                        header = header..header_more
                        _,left = header:unpack('>I')
--                        length = left
--                        print(left)
                        if left == 0 then
--                           print('on message')
                           on_message(parts)
                           parts = {}
                           part = nil
                           left = nil
                           length = nil
                           header = nil
                        else
                           length = left
--                           print('length',length)
                        end
                     end
                     if length then
 --                       print('aaa')
                        if not part or #part ~= length then
  --                      print('xxx')
                           local last = part
                           local err,sub
                           part,err,sub = client:receive(left)                           
                           if err then
--                              print('err 2',err)
                              if err == 'timeout' then
                                 if part then
                                    part = part..sub
                                 else
                                    part = sub
                                 end
                                 left = length - #part
                                 return
                              else -- if err == 'closed' then
                                 close()
                                 read_io:stop(loop)
                                 return
                              end
                           end
                           if last then
                              part = last..part
                           end
                           if #part == length then
--                              print('on complete')
                              tinsert(parts,part)
                              part = nil
                              left = nil
                              length = nil
                              header = nil
                              --return 
                           end
                        end
                     end
                  end
               end,
               client:getfd(),
               ev.READ
            ):start(loop)
         end,
         sock:getfd(),
         ev.READ
      )
      return {
         io = listen_io
      }
   end

return {
   listener = listener
}

