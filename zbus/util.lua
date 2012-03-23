local ev = require'ev'
local zmq = require'zmq'
local zEVENTS = zmq.EVENTS
local zIN = zmq.POLLIN
local zINOUT = zmq.POLLIN + zmq.POLLOUT

module('zbus.util')

local zmq_read_io = 
   function(zsock,on_readable)
      local getopt = zsock.getopt
      return ev.IO.new(
         function(loop,io)
            while true do
               local events = getopt(zsock,zEVENTS)
               if events == zIN or events == zINOUT then      
                  on_readable(loop,io)
               else
                  break
               end
            end
         end,
         getopt(zsock,zmq.FD),
         ev.READ)
   end

local zmethods
local zmq_methods =
   function(zcontext)
      if not zmethods then
         local t = zcontext:socket(zmq.REP)
         zmethods = {
            recv = t.recv,
            recv_msg = t.recv_msg,
            send = t.send,
            send_msg = t.send_msg,
            getopt = t.getopt
         }
         t:close()
      end
      return zmethods
   end

return {
   zmq_read_io = zmq_read_io,
   zmq_methods = zmq_methods
}


