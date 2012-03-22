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

return {
   zmq_read_io = zmq_read_io
}
