local ev = require'ev'
local zmq = require'zmq'
local zEVENTS = zmq.EVENTS
local zIN = zmq.POLLIN
local zINOUT = zmq.POLLIN + zmq.POLLOUT
local print = print
local pairs = pairs
module('zbus.util')

local zsocks = {}

local idle = ev.Idle.new(
   function(loop,io)
      io:stop(loop)
      for sock,entry in pairs(zsocks) do         
         entry.cb(entry.loop,entry.io)
      end
   end)

local add_read_io = 
   function(zsock,on_readable)
      local getopt = zsock.getopt
      local cb = 
         function(loop,io)
            while true do
               local events = getopt(zsock,zEVENTS)
               if events == zIN or events == zINOUT then      
                  on_readable(loop,io)
               else
                  break
               end
            end
         end
      zsocks[zsock] = {}
      zsocks[zsock].cb = cb
      local is_init
      return ev.IO.new(
         function(loop,io)
            if not is_init then
               zsocks[zsock].loop = loop
               zsocks[zsock].io = io
               is_init = true
            end
            cb(loop,io)
         end,
         getopt(zsock,zmq.FD),
         ev.READ)
   end

local remove_read_io = 
   function(zsock)
      zsocks[zsock] = nil
   end

local zmethods
local zmq_methods =
   function(zcontext)
      if not zmethods then
         local t = zcontext:socket(zmq.REP)        
         local send = t.send
         local send_msg = t.send_msg
         zmethods = {
            recv = t.recv,
            recv_msg = t.recv_msg,
            send = function(...) 
                      if not idle:is_active() then
                         idle:start(ev.Loop.default)
                      end
                      return send(...)
                   end,
            send_msg = function(...) 
                          if not idle:is_active() then
                             idle:start(ev.Loop.default)
                          end
                          return send_msg(...)
                       end,
            getopt = t.getopt
         }
         t:close()
      end
      return zmethods
   end

return {
   add_read_io = add_read_io,
   remove_read_io = remove_read_io,
   zmq_methods = zmq_methods
}


