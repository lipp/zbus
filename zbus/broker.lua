local zmq = require'zmq'
local zpoller = require'zmq.poller'
local assert = assert
local table = table
local pairs = pairs
local type = type
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
local config = require'zbus.config'

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
      config = config.broker(user)
      local log = config.log 
      self.reg = zcontext:socket(zmq.REP)
      self.reg:bind('tcp://*:'..config.broker.registry_port)
      self.router = zcontext:socket(zmq.XREP)
      log(self.router,self.reg)
      self.router:bind('tcp://*:'..config.broker.rpc_port)
      self.notify = zcontext:socket(zmq.PULL)
      local notify_url = 'tcp://*:'..config.broker.notify_port
      self.notify:bind(notify_url)
      self.poller = config.poller or zpoller(2)
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
      
      local send = self.router.send 
      local recv = self.router.recv
      local send_msg = self.router.send_msg
      local recv_msg = self.router.recv_msg
      local getopt = self.router.getopt
      self.new_replier = 
         function(self,url)
            local replier = {}
            replier.exps = {}
            replier.dealer = zcontext:socket(zmq.XREQ)
            replier.dealer:connect(url)
            local dealer = replier.dealer
            local router = self.router
            local part_msg = zmq_init_msg() 
            self.poller:add(
               replier.dealer,zmq.POLLIN,
               function()
                  local more              
                  repeat
                     recv_msg(dealer,part_msg)
                     more = getopt(dealer,zRCVMORE) > 0
                     send_msg(router,part_msg,more and zSNDMORE or 0)
                     print('asdd')
                  until not more
               end) 
            self.repliers[url] = replier
         end

      self.new_listener = 
         function(self,url)
            local listener = {}
            listener.exps = {}
            listener.push = zcontext:socket(zmq.PUSH)
            listener.push:connect(url)
            self.listeners[url] = listener
         end

      self.reg_calls = {
         url_get = 
            function()
               return self.url_pool:get()
            end,
         url_release = 
            function(url)
               self.url_pool:release(url)
            end,
         replier_open = 
            function(replier_url)
               if not replier_url then
                  error('argument error')
               end
               if self.repliers[replier_url] then
                  error('replier already open:'..replier_url)
               end 
               self:new_replier(replier_url)
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
                  self.poller:remove(replier.dealer)
                  while true do 
                     local events = replier.dealer:getopt(zEVENTS)
                     if events ~= zIN and events ~= zINOUT then
                        break
                     end
                     local more
                     repeat
                        local part = replier.dealer:recv()
                        more = replier.dealer:getopt(zRCVMORE) > 0
                        self.router:send(part,more and zSNDMORE or 0)
                     until not more
                  end
                  replier.dealer:close()
                  self.repliers[replier_url] = nil
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
            function(listen_url)
               if not listen_url then
                  error('argument error')
               end
               if self.listeners[listen_url] then
                  error('listener already open:'..listen_url)
               end
               self:new_listener(listen_url)                    
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

      local dispatch_reg = 
         function()
            local cmd = self.reg:recv()
            local args = {}
            while self.reg:getopt(zRCVMORE) > 0 do
               local arg = self.reg:recv()
               table.insert(args,arg)
            end
            log('reg',cmd,unpack(args))
            local ok,ret = pcall(self.reg_calls[cmd],unpack(args))        
            log('reg','=>',ok,ret)        
            if not ok then
               self.reg:send('',zSNDMORE)
               self.reg:send(ret)
            else
               ret = ret or ''
               self.reg:send(ret)
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
            send(router,err_id,zSNDMORE)        
            send(router,err,0)                	   
         end

      local router = self.router
      local forward_rpc =
         function()
            recv_msg(router,xid_msg)
            if getopt(router,zRCVMORE) < 1 then
               send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_EMPTY_PART')
            end
            recv_msg(router,empty_msg)
            assert(empty_msg:size()==0)
            if getopt(router,zRCVMORE) < 1 then
               send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_METHOD')
            end
            recv_msg(router,method_msg)
            if getopt(router,zRCVMORE) < 1 then
               send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_ARG')
            end
            recv_msg(router,arg_msg)        
            --        log('rpc',method,arg)
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
               log('BAEM')
               send_msg(dealer,xid_msg,zSNDMORE)
               send_msg(dealer,empty_msg,zSNDMORE)
               send(dealer,matched_exp,zSNDMORE)
               send_msg(dealer,method_msg,zSNDMORE)
               send_msg(dealer,arg_msg,0)          
               log('BAEM BEAM')
            end
         end  

      local data_msg = zmq_init_msg()
      local topic_msg = zmq_init_msg()
      local notify = self.notify
      local listeners = self.listeners   
      local forward_notify = 
         function()
            local todos = {}
            repeat
               recv_msg(notify,topic_msg)	   
               if getopt(notify,zRCVMORE) < 1 then
                  log('notify','invalid message','no_data')
                  break
               end
               local topic = tostring(topic_msg)
               recv_msg(notify,data_msg)
               for url,listener in pairs(listeners) do
                  for _,exp in pairs(listener.exps) do
                     log('notify','trying',topic)
                     if smatch(topic,exp) then
                        if not todos[listener] then
                           todos[listener] = {}
                        end
                        tinsert(todos[listener],{
                                   exp,
                                   zmq_copy_msg(topic_msg),
                                   zmq_copy_msg(data_msg),
                                })
                        log('notify','matched',topic,exp,url)		     		    
                     end
                  end
               end
            until getopt(notify,zRCVMORE) <= 0
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
      
      self.forward_rpc = forward_rpc
      self.dispatch_reg = dispatch_reg
      self.forward_notify = forward_notify
      self.loop = 
         function(self)
            self.poller:add(
               self.reg,zmq.POLLIN,
               self.dispatch_reg)
            self.poller:add(
               self.router,zmq.POLLIN,
               self.forward_rpc)
            self.poller:add(
               self.notify,zmq.POLLIN,
               self.forward_notify)
            self.poller:start()
         end
      return self
   end

return {
   new = new
}
