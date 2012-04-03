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

local url_pool = 
   function(interface,port_min,port_max)
      local self = {}
      self.free = {}
      self.used = {}
      self.url_base = 'tcp://'..interface..':'
      for port = port_min,port_max do       
         self.free[self.url_base..port] = true
      end

      self.get = 
         function(self)
            -- get first table element
            local url = pairs(self.free)(self.free)
            if url then
               self.free[url] = nil
               self.used[url] = true
               return url
            else
               error('url pool empty')
            end
         end

      self.release =
         function(self,url)	 
            if self.used[url] then
               self.used[url] = nil
               self.free[url] = true
               return true
            else
               error('invalid url')
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
      self.url_pool = url_pool(
         config.url_pool.interface,
         config.url_pool.port_min,
         config.url_pool.port_max
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
      local send = zmethods.send 
      local recv = zmethods.recv
      local send_msg = zmethods.send_msg
      local recv_msg = zmethods.recv_msg
      local getopt = zmethods.getopt

      self.new_replier = 
         function(self)
            local replier = {}
            local url = self.url_pool:get()
            replier.exps = {}
            replier.dealer = zcontext:socket(zmq.XREQ)
            replier.dealer:bind(url)
            local dealer = replier.dealer
            local router = self.method_socket
            local part_msg = zmq_init_msg()
            replier.io = zutil.zmq_read_io(
               dealer,
               function()
                  local more              
                  repeat
                     recv_msg(dealer,part_msg)
                     more = getopt(dealer,zRCVMORE) > 0
                     send_msg(router,part_msg,more and zSNDMORE or 0)
                  until not more
               end) 
            replier.io:start(loop)
            self.repliers[url] = replier
            return url
         end

      self.new_listener = 
         function(self)
            local listener = {}
            local url = self.url_pool:get()
            listener.exps = {}
            listener.push = zcontext:socket(zmq.PUSH)
            listener.push:bind(url)
            self.listeners[url] = listener
            return url
         end

      self.registry_calls = {
         replier_open = 
            function()
               return self:new_replier(replier_url)
            end,

         replier_close = 
            function(replier_url)
               if not replier_url then
                  error('argument error')
               end
               if not self.repliers[replier_url] then
                  error('no replier:'..replier_url)
               end 
               local replier = self.repliers[replier_url]
               if replier then            
                  replier.io:stop(loop)
                  while true do 
                     local events = replier.dealer:getopt(zEVENTS)
                     if events ~= zIN and events ~= zINOUT then
                        break
                     end
                     local more
                     repeat
                        local part = replier.dealer:recv()
                        more = replier.dealer:getopt(zRCVMORE) > 0
                        self.method_socket:send(part,more and zSNDMORE or 0)
                     until not more
                  end
                  replier.dealer:close()
                  self.repliers[replier_url] = nil
                  self.url_pool:release(replier_url)
               end
            end,

         replier_add = 
            function(replier_url,exp)
               if not replier_url or not exp then
                  error('argument error')
               end
               if not self.repliers[replier_url] then
                  error('no replier:'..replier_url)
               end
               table.insert(self.repliers[replier_url].exps,exp)              
            end,

         replier_remove = 
            function(replier_url,exp)
               if not replier_url or not exp then
                  error('argument error')
               end          
               local rep = self.repliers[replier_url]
               if not rep then
                  error('no replier:'..replier_url)
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
               return self:new_listener(listen_url)                    
            end,

         listen_close = 
            function(listen_url)
               if not listen_url then
                  error('argument error')
               end
               if not self.listeners[listen_url] then
                  error('no listener open:'..listen_url)
               end
               self.listeners[listen_url].push:close()
               self.listeners[listen_url] = nil
               self.url_pool:release(listen_url)
            end,

         listen_add = 
            function(listen_url,exp)
               if not listen_url or not exp then
                  error('argument error')
               end
               if not self.listeners[listen_url] then
                  error('no listener open:'..listen_url)
               end
               table.insert(self.listeners[listen_url].exps,exp)          
            end,

         listen_remove = 
            function(listen_url,exp)
               if not listen_url or not exp then
                  error('arguments error')
               end
               local listener = self.listeners[listen_url]
               if not listener then
                  error('no listener open:'..listen_url)
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
            local cmd = self.registry_socket:recv()
            local args = {}
            while self.registry_socket:getopt(zRCVMORE) > 0 do
               local arg = self.registry_socket:recv()
               table.insert(args,arg)
            end
            log('reg',cmd,unpack(args))
            local ok,ret = pcall(self.registry_calls[cmd],unpack(args))        
            log('reg','=>',ok,ret)        
            if not ok then
               self.registry_socket:send('',zSNDMORE)
               self.registry_socket:send(ret)
            else
               ret = ret or ''
               self.registry_socket:send(ret)
            end
         end

      local xid_msg = zmq_init_msg()
      local empty_msg = zmq_init_msg()
      local method_msg = zmq_init_msg()
      local arg_msg = zmq_init_msg()

      local send_error = 
         function(router,xid_msg,err_id,err)
            send_msg(router,xid_msg,zSNDMORE)
            send(router,'',zSNDMORE)
            send(router,'',zSNDMORE)
            send(router,err_id or 'UNKNOWN_ERROR',zSNDMORE)        
            send(router,err or '',0)                	   
         end

      local router = self.method_socket
      local forward_method_call =
         function()
            local more
            repeat 
               recv_msg(router,xid_msg)
               if getopt(router,zRCVMORE) < 1 then
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_EMPTY_PART')
               end
               recv_msg(router,empty_msg)
               if empty_msg:size() > 0 then
                  log('forward_method_call','protocol error','expected empty message part')
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','EMPTY_PART_EXPECTED')
               end
               if getopt(router,zRCVMORE) < 1 then
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_METHOD')
               end
               recv_msg(router,method_msg)
               if getopt(router,zRCVMORE) < 1 then
                  send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_ARG')
               end
               recv_msg(router,arg_msg)        
               local dealer,matched_exp
               local err,err_id
               local method = tostring(method_msg)
               for url,replier in pairs(self.repliers) do 
                  for _,exp in pairs(replier.exps) do
                     log('rpc','trying',exp,method)--,method:match(exp))
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
                  send_msg(dealer,xid_msg,zSNDMORE)
                  send_msg(dealer,empty_msg,zSNDMORE)
                  send(dealer,matched_exp,zSNDMORE)
                  send_msg(dealer,method_msg,zSNDMORE)
                  send_msg(dealer,arg_msg,0)          
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
               recv_msg(notification_socket,topic_msg)	   
               if getopt(notification_socket,zRCVMORE) < 1 then
                  log('forward_notifications','invalid message','no_data')
                  break
               end
               local topic = tostring(topic_msg)
               recv_msg(notification_socket,data_msg)
               for url,listener in pairs(listeners) do
                  for _,exp in pairs(listener.exps) do
                     log('forward_notifications','trying',topic)
                     if smatch(topic,exp) then
                        if not todos[listener] then
                           todos[listener] = {}
                        end
                        tinsert(todos[listener],{
                                   exp,
                                   zmq_copy_msg(topic_msg),
                                   zmq_copy_msg(data_msg),
                                })
                        log('forward_notifications','matched',topic,exp,url)		     		    
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
                  send(lpush,notification[1],zSNDMORE)
                  send_msg(lpush,notification[2],zSNDMORE)
                  send_msg(lpush,notification[3],option)
               end
            end
         end
      
      self.forward_method_call = forward_method_call
      self.dispatch_registry_call = dispatch_registry_call
      self.forward_notifications = forward_notifications
      self.loop = 
         function(self)
            local registry_io = zutil.zmq_read_io(self.registry_socket,self.dispatch_registry_call)
            local rpc_io = zutil.zmq_read_io(self.method_socket,self.forward_method_call)
            local notification_io = zutil.zmq_read_io(self.notification_socket,self.forward_notifications)
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
