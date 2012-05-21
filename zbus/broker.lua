local zmq = require'zmq'
local ev = require'ev'
local assert = assert
local table = table
local pairs = pairs
local tostring = tostring
local ipairs = ipairs
local unpack = unpack
local pcall = pcall
local error = error
local print = print
local require = require
local cjson = require'cjson'
local os = require'os'
local tconcat = table.concat
local tinsert = table.insert
local smatch = string.match
local zconfig = require'zbus.config'
local zutil = require'zbus.util'

module('zbus.broker')

local zcontext = zmq.init(1)
local zIN = zmq.POLLIN
local zINOUT = zmq.POLLIN + zmq.POLLOUT
local zSNDMORE = zmq.SNDMORE
local zRCVMORE = zmq.RCVMORE
local zEVENTS = zmq.EVENTS
local zNOBLOCK = zmq.NOBLOCK

local port_pool = 
   function(port_min,port_max)
      local self = {}
      self.free = {}
      self.used = {}
      for port=port_min,port_max do       
         self.free[tostring(port)] = true
      end

      self.get = 
         function(self)
            -- get first table element
            local port = pairs(self.free)(self.free)
            if port then
               self.free[port] = nil
               self.used[port] = true
               return port
            else
               error('port pool empty')
            end
         end

      self.release =
         function(self,port)	 
            if self.used[port] then
               self.used[port] = nil
               self.free[port] = true
               return true
            else
               error('invalid port')
            end
         end

      return self
   end

new = 
   function(user)
      local self = {}
      local config = zconfig.broker(user)
      local log = config.log 
      local loop = ev.Loop.default
      self.registry_socket = zcontext:socket(zmq.REP)
      self.registry_socket:bind('tcp://*:'..config.broker.registry_port)
      self.method_socket = zcontext:socket(zmq.XREP)
      log(self.method_socket,self.registry_socket)
      self.method_socket:bind('tcp://*:'..config.broker.rpc_port)
      self.notification_socket = zcontext:socket(zmq.PULL)
      local notify_url = 'tcp://*:'..config.broker.notify_port
      self.notification_socket:bind(notify_url)
      self.repliers = {}
      self.listeners = {}
      self.port_pool = port_pool(
         config.port_pool.port_min,
         config.port_pool.port_max
      )

      local smatch = smatch
      local zmq_init_msg = zmq.zmq_msg_t.init
      local zmq_copy_msg = 
         function(msg)
            local cmsg = zmq_init_msg()
            cmsg:copy(msg)
            return cmsg
         end
      
      local zmethods = zutil.zmq_methods(zcontext)
      
      if config.debug then
         local asserted_zmethods = {}
         for name,f in pairs(zmethods) do
            asserted_zmethods[name] = 
               function(...)
                  return assert(f(...))
               end                        
         end
         zmethods = asserted_zmethods
      end

      local send = zmethods.send
      local recv = zmethods.recv
      local send_msg = zmethods.send_msg
      local recv_msg = zmethods.recv_msg
      local getopt = zmethods.getopt

      self.registry_calls = {
         replier_open = 
            function()
               local replier = {}
               local port = self.port_pool:get()
               local url = 'tcp://'..config.broker.interface..':'..port
               replier.exps = {}
               replier.dealer = zcontext:socket(zmq.XREQ)
               replier.dealer:bind(url)
               local dealer = replier.dealer
               local router = self.method_socket
               local part_msg = zmq_init_msg()
               replier.io = zutil.add_read_io(
                  dealer,
                  function()
                     local more              
                     repeat
                        recv_msg(dealer,part_msg,zNOBLOCK)
                        more = getopt(dealer,zRCVMORE) > 0
                        send_msg(router,part_msg,(more and zSNDMORE or 0) + zNOBLOCK)
                     until not more
                  end) 
               replier.io:start(loop)
               self.repliers[port] = replier
               return port
            end,

         replier_close = 
            function(replier_port)
               if not replier_port then
                  error('argument error')
               end
               if not self.repliers[replier_port] then
                  error('no replier:'..replier_port)
               end 
               local replier = self.repliers[replier_port]
               if replier then            
                  replier.io:stop(loop)
                  while true do 
                     local events = getopt(replier.dealer,zEVENTS)
                     if events ~= zIN and events ~= zINOUT then
                        break
                     end
                     local more
                     repeat
                        local part = recv(replier.dealer,zNOBLOCK)
                        more = getopt(replier.dealer,zRCVMORE) > 0
                        send(self.method_socket,part,more and zSNDMORE or 0)
                     until not more
                  end
                  replier.dealer:close()
                  zutil.remove_read_io(replier.dealer)
                  self.repliers[replier_port] = nil
                  self.port_pool:release(replier_port)
               end
            end,

         replier_add = 
            function(replier_port,exp)
               if not replier_port or not exp then
                  error('argument error')
               end
               if not self.repliers[replier_port] then
                  error('no replier:'..replier_port)
               end
               table.insert(self.repliers[replier_port].exps,exp)              
            end,

         replier_remove = 
            function(replier_port,exp)
               if not replier_port or not exp then
                  error('argument error')
               end          
               local rep = self.repliers[replier_port]
               if not rep then
                  error('no replier:'..replier_port)
               end
               local ok
               for i=1,#rep.exps do
                  if rep.exps[i] == exp then
                     table.remove(rep.exps,i)
                     ok = true
                  end
               end
               if not ok then
                  error('unknown expression:',exp)
               end
            end,

         listen_open = 
            function()
               local listener = {}
               local port = self.port_pool:get()
               local url = 'tcp://'..config.broker.interface..':'..port
               listener.exps = {}
               listener.push = zcontext:socket(zmq.PUSH)
               listener.push:bind(url)
               self.listeners[port] = listener
               return port
            end,

         listen_close = 
            function(listen_port)
               if not listen_port then
                  error('argument error')
               end
               if not self.listeners[listen_port] then
                  error('no listener open:'..listen_port)
               end
               self.listeners[listen_port].push:close()
               self.listeners[listen_port] = nil
               self.port_pool:release(listen_port)
            end,

         listen_add = 
            function(listen_port,exp)
               if not listen_port or not exp then
                  error('argument error')
               end
               if not self.listeners[listen_port] then
                  error('no listener open:'..listen_port)
               end
               table.insert(self.listeners[listen_port].exps,exp)          
            end,

         listen_remove = 
            function(listen_port,exp)
               if not listen_port or not exp then
                  error('arguments error')
               end
               local listener = self.listeners[listen_port]
               if not listener then
                  error('no listener open:'..listen_port)
               end
               for i=1,#listener.exps do
                  if listener.exps[i] == exp then
                     table.remove(listener.exps,i)
                  end
               end        
            end,
      }

      local dispatch_registry_call = 
         function()
            local cmd = recv(self.registry_socket,zNOBLOCK)
            local args = {}
            while getopt(self.registry_socket,zRCVMORE) > 0 do
               local arg = recv(self.registry_socket,zNOBLOCK)
               table.insert(args,arg)
            end
--            log('reg',cmd,unpack(args))
            local ok,ret = pcall(self.registry_calls[cmd],unpack(args))        
--            log('reg','=>',ok,ret)        
            if not ok then
               send(self.registry_socket,'',zSNDMORE)
               send(self.registry_socket,ret)
            else
               ret = ret or ''
               send(self.registry_socket,ret)
            end
         end

      local xid_msg = zmq_init_msg()
      local empty_msg = zmq_init_msg()
      local method_msg = zmq_init_msg()
      local arg_msg = zmq_init_msg()

      local send_error = 
         function(router,xid_msg,err_id,err)
            send_msg(router,xid_msg,zSNDMORE + zNOBLOCK)
            send(router,'',zSNDMORE + zNOBLOCK)
            send(router,'',zSNDMORE + zNOBLOCK)
            send(router,err_id or 'UNKNOWN_ERROR',zSNDMORE + zNOBLOCK)        
            send(router,err or '',zNOBLOCK)                	   
         end

      local router = self.method_socket
      local forward_method_call =
         function()
--            log('forward_method_call','in')
            local more
            repeat 
               recv_msg(router,xid_msg,zNOBLOCK)
               if getopt(router,zRCVMORE) < 1 then
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_EMPTY_PART')
               end
               recv_msg(router,empty_msg,zNOBLOCK)
               if empty_msg:size() > 0 then
                  log('forward_method_call','protocol error','expected empty message part')
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','EMPTY_PART_EXPECTED')
               end
               if getopt(router,zRCVMORE) < 1 then
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_METHOD')
               end
               recv_msg(router,method_msg,zNOBLOCK)
               if getopt(router,zRCVMORE) < 1 then
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_ARG')
               end
               recv_msg(router,arg_msg,zNOBLOCK)        
               local dealer,matched_exp
               local err,err_id
               local method = tostring(method_msg)
               for url,replier in pairs(self.repliers) do 
                  for _,exp in pairs(replier.exps) do
--                     log('rpc','trying XX',exp,method)--,method:match(exp))
                     if smatch(method,exp) then
                        log('rpc','matched',method,exp,url)
                        if dealer then
                           log('rpc','method ambiguous',method,exp,url)
                           err = 'method ambiguous: '..method
                           err_id = 'ERR_AMBIGUOUS'
                        else
                           matched_exp = exp
                           dealer = replier.dealer
                        end
                     end
                  end
               end
               if not dealer then
                  err = 'no method for '..method
                  err_id = 'ERR_NOMATCH'
               end
               if err then
                  send_error(router,xid_msg,err_id,err)
               else
                  send_msg(dealer,xid_msg,zSNDMORE + zNOBLOCK)
                  send_msg(dealer,empty_msg,zSNDMORE + zNOBLOCK)
                  send(dealer,matched_exp,zSNDMORE + zNOBLOCK)
                  send_msg(dealer,method_msg,zSNDMORE + zNOBLOCK)
                  send_msg(dealer,arg_msg,0 + zNOBLOCK)          
               end
               more = getopt(router,zRCVMORE) > 0
            until not more
         end  

      local data_msg = zmq_init_msg()
      local topic_msg = zmq_init_msg()
      local notification_socket = self.notification_socket
      local listeners = self.listeners   
      local forward_notifications = 
         function()
            local todos = {}
            repeat
               recv_msg(notification_socket,topic_msg,zNOBLOCK)	   
               if getopt(notification_socket,zRCVMORE) < 1 then
                  log('forward_notifications','invalid message','no_data')
                  break
               end
               local topic = tostring(topic_msg)
               recv_msg(notification_socket,data_msg,zNOBLOCK)
               for url,listener in pairs(listeners) do
                  for _,exp in pairs(listener.exps) do
--                     log('forward_notifications','trying XX',topic)
                     if smatch(topic,exp) then
                        if not todos[listener] then
                           todos[listener] = {}
                        end
                        tinsert(todos[listener],{
                                   exp,
                                   zmq_copy_msg(topic_msg),
                                   zmq_copy_msg(data_msg),
                                })
--                        log('forward_notifications','matched',topic,exp,url)		     		    
                     end
                  end
               end
            until getopt(notification_socket,zRCVMORE) <= 0
            for listener,notifications in pairs(todos) do
               local len = #notifications
               local lpush = listener.push
               for i,notification in ipairs(notifications) do
                  local option = zSNDMORE
                  if i == len then
                     option = 0
                  end
                  send(lpush,notification[1],zSNDMORE + zNOBLOCK)
                  send_msg(lpush,notification[2],zSNDMORE + zNOBLOCK)
                  send_msg(lpush,notification[3],option + zNOBLOCK)
               end
            end
         end
      
      self.forward_method_call = forward_method_call
      self.dispatch_registry_call = dispatch_registry_call
      self.forward_notifications = forward_notifications
      self.loop = 
         function(self)
            local registry_io = zutil.add_read_io(self.registry_socket,self.dispatch_registry_call)
            local rpc_io = zutil.add_read_io(self.method_socket,self.forward_method_call)
            local notification_io = zutil.add_read_io(self.notification_socket,self.forward_notifications)
            rpc_io:start(loop)
            notification_io:start(loop)
            registry_io:start(loop)
            loop:loop()
         end
      
      return self
   end

return {
   new = new
}
