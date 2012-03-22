local zmq = require'zmq'
local ev = require'ev'
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
local zconfig = require'zbus.config'

module('zbus.member')

local zcontext = zmq.init(1)
local zSNDMORE = zmq.SNDMORE
local zRCVMORE = zmq.RCVMORE
local zEVENTS = zmq.EVENTS
local zIN = zmq.POLLIN
local zINOUT = zmq.POLLIN + zmq.POLLOUT

new = 
   function(user)
      local config = zconfig.member(user)
      local self = {}        
      local log = config.log
      local serialize_args = config.serialize.args
      local serialize_result = config.serialize.result
      local serialize_err = config.serialize.err
      local make_zerr = config.make_err
      local unserialize_args = config.unserialize.args
      local unserialize_result = config.unserialize.result
      local unserialize_err = config.unserialize.err
      local smatch = smatch
      self.broker_call = 
         function(self,args)
            if not self.broker_reg then
               self.broker_reg = zcontext:socket(zmq.REQ) 
               local broker_url = 'tcp://'..config.broker.ip..':'..config.broker.registry_port
               self.broker_reg:connect(broker_url)
            end
            for i,arg in ipairs(args) do          
               self.broker_reg:send(arg,zSNDMORE)
            end
            self.broker_reg:send(config.name)
            local resp = self.broker_reg:recv()
            if self.broker_reg:getopt(zRCVMORE) > 0 then
               error('broker call "'..tconcat(args,',')..'" failed:'..self.broker_reg:recv())
            else
               --          log('broker_call',unpack(args),'=>',resp)
               return resp
            end
         end

      self.listen_init = 
         function(self)        
            assert(not self.listen)
            self.listen_url = self:broker_call{'url_get'}
            self.listen = zcontext:socket(zmq.PULL)
            self.listen:bind(self.listen_url)
            self:broker_call{'listen_open',self.listen_url}
            self.listen_callbacks = {}          
         end

      self.listen_add = 
         function(self,expr,func)
            if not self.listen then
               self:listen_init()
            end
            self:broker_call{'listen_add',self.listen_url,expr}
            self.listen_callbacks[expr] = func
         end
      self.listen_remove = 
         function(self,expr)
            if not self.listen then
               return
            end
            self:broker_call{'listen_remove',self.listen_url,expr}
            if expr then
               self.listen_callbacks[expr] = nil
            end
         end

      self.replier_init = 
         function(self)
            assert(not self.rep)
            self.rep_url = self:broker_call{'url_get'}
            self.rep = zcontext:socket(zmq.REP)
            self.rep:bind(self.rep_url)
            self:broker_call{'replier_open',self.rep_url}
            self.reply_callbacks = {}
         end
      
      self.replier_add = 
         function(self,expr,func,async)
            if not self.rep then
               self:replier_init()
            end
            self:broker_call{'replier_add',self.rep_url,expr}
            self.reply_callbacks[expr] = {
               func = func,
               async = async or false
            }
         end
      self.replier_remove = 
         function(self,expr)
            if not self.rep then
               return
            end
            self:broker_call{'replier_remove',self.rep_url,expr}
            if expr then
               self.reply_callbacks[expr] = nil
            end
         end
      local rep
      local recv
      local send
      local reply_callbacks
      self.handle_req = 
         function()        
            log('handle_req')
            rep = rep or self.rep
            recv = recv or rep.recv
            send = send or rep.send
            reply_callbacks = reply_callbacks or self.reply_callbacks
            local expr = recv(rep)
            local method = recv(rep)
            local arguments = recv(rep)        
            --	assert(reply_callbacks[expr])
            --        log('handle_req',expr,method,arguments,'async',self.reply_callbacks[expr].async)
            local on_success = 
               function(...)
                  send(rep,serialize_result(...))  
               end
            local on_error = 
               function(err)
                  send(rep,'',zSNDMORE)          
                  send(rep,serialize_err(err))
               end        
            local result        
            if reply_callbacks[expr].async then
               result = {pcall(reply_callbacks[expr].func,
                               method,
                               on_success,
                               on_error,
                               unserialize_args(arguments))}
            else
               result = {pcall(reply_callbacks[expr].func,
                               method,
                               unserialize_args(arguments))}
               if result[1] then 
                  on_success(unpack(result,2))
                  return
               end
            end
            if not result[1] then 
               -- we assume that something terrible happened and
               -- the reply_callback will not call on_error as expected
               --          log('handle_req',method,'returned error',tostring(result[2]))
               on_error(result[2])
            end                
         end
      local listen
      self.handle_notify = 
         function()
            listen = listen or self.listen
            recv = recv or listen.recv
            getopt = getopt or listen.getopt
            local listen_callbacks
            local more
            repeat 
               local expr = recv(listen)
               local topic = recv(listen)
               local arguments = recv(listen)
               listen_callbacks = listen_callbacks or self.listen_callbacks
               more = getopt(listen,zRCVMORE) > 0
               local cb = listen_callbacks[expr]
               -- could be removed in the meantime
               if cb then
                  local ok,err = pcall(cb,topic,more,unserialize_args(arguments))
                  if not ok then
                     log('handle_notify callback failed',expr,err)
                  end
               end
            until not more
         end
      
      self.notify = 
         function(self,topic,...)
            self:notify_more(topic,false,...)
         end

      self.notify_more = 
         function(self,topic,more,...)
            local option = 0        
            if more then
               option = zSNDMORE
            end
            local nsock = self.notify_sock
            if not nsock then
               self.notify_sock = zcontext:socket(zmq.PUSH)
               local notify_url = 'tcp://'..config.broker.ip..':'..config.broker.notify_port
               self.notify_sock:connect(notify_url)    
               nsock = self.notify_sock
            end
            send = send or nsock.send
            send(nsock,topic,zSNDMORE)
            send(nsock,serialize_args(...),option)
         end
      
      
      self.close = 
         function(self)        
            if self.listen then
               self:broker_call{'listen_close',self.listen_url}
            end
            if self.rep then
               self:broker_call{'replier_close',self.rep_url}
            end
            if self.notify_sock then 
               self.notify_sock:close() 
            end
            if self.listen then 
               self.listen:close() 
               self:broker_call{'url_release',self.listen_url}
            end
            if self.rep then 
               self.rep:close() 
               self:broker_call{'url_release',self.rep_url}
            end
            if self.broker_reg then 
               self.broker_reg:close() 
            end
            if self.rpc_socks then 
               for _,sock in pairs(self.rpc_socks) do
                  sock:close() 
               end
            end
         end

      self.reply_io = 
         function(self)
            if not self.rep then self:replier_init() end
            return ev.IO.new(
               function(loop,io)
                  while true do
                     local events = self.rep:getopt(zEVENTS)
                     if events == zIN or events == zINOUT then      
                        self.handle_req()
                     else
                        break
                     end
                  end
               end,
               self.rep:getopt(zmq.FD),
               ev.READ)      
         end
      
      self.listen_io = 
         function(self)
            if not self.listen then self:listen_init() end
            return ev.IO.new(
               function(loop,io)
                  while true do
                     local events = self.listen:getopt(zEVENTS)
                     if events == zIN or events == zINOUT then      
                        self.handle_notify()
                     else
                        break
                     end
                  end
               end,
               self.listen:getopt(zmq.FD),
               ev.READ)     
         end



      self.rpc_socks = {}
      self.rpc_local_url = 'tcp://'..config.broker.ip..':'..config.broker.rpc_port
      self.call_url = 
         function(self,url,method,...)
            if not self.rpc_socks[url] then
               self.rpc_socks[url] = zcontext:socket(zmq.REQ)
               self.rpc_socks[url]:connect(url)
            end
            local sock = self.rpc_socks[url]
            sock:send(method,zSNDMORE)
            sock:send(serialize_args(...))
            local resp = sock:recv()
            if sock:getopt(zRCVMORE) > 0 then
               local err = sock:recv()
               if sock:getopt(zRCVMORE) > 0 then            
                  local msg = sock:recv()
                  error(make_zerr(err,msg),2)
               else
                  error(unserialize_err(err),2)
               end
            end
            return unserialize_result(resp)
         end

      self.call = 
         function(self,method,...)
            return self:call_url(self.rpc_local_url,method,...)
         end

      self.loop = 
         function(self,options)
            local options = options or {}
            local listen_io = self:listen_io()
            local reply_io = self:reply_io()
            local loop = options.ev_loop or ev.Loop.default
            self.loop = loop
            local SIGHUP = 1
            local SIGINT = 2
            local SIGKILL = 9
            local SIGTERM = 15	
            local quit = 
               function()
                  if options.exit then
                     options.exit()
                  end
                  self:close()
                  zcontext:term()
                  if listen_io then listen_io:stop(loop) end
                  if reply_io then reply_io:stop(loop) end
                  if options.ios then
                     for _,io in ipairs(options.ios) do
                        io:stop(loop)
                     end
                  end
               end      
            local quit_and_exit = 
               function()
                  quit()
                  os.exit()
               end
            ev.Signal.new(quit_and_exit,SIGHUP):start(loop)
            ev.Signal.new(quit_and_exit,SIGINT):start(loop)
            ev.Signal.new(quit_and_exit,SIGKILL):start(loop)
            ev.Signal.new(quit_and_exit,SIGTERM):start(loop)  
            if listen_io then 
               log('LISTEN');
               listen_io:start(loop) 
            end
            if reply_io then 
               log('REPLY');
               reply_io:start(loop) 
            end
            if options.ios then
               for _,io in ipairs(options.ios) do
                  io:start(loop)
               end
            end
            loop:loop()
            quit()
         end

      self.unloop = 
         function(self)
            self.loop:unloop()
         end
      
      return self
   end

return {
   new = new
}
