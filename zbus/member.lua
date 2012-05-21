local zmq = require'zmq'
local ev = require'ev'
local assert = assert
local table = table
local pairs = pairs
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
local zconfig = require'zbus.config'
local zutil = require'zbus.util'

module('zbus.member')

local zcontext = zmq.init(1)
local zSNDMORE = zmq.SNDMORE
local zRCVMORE = zmq.RCVMORE

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
      self.ev_loop = config.ev_loop or ev.Loop.default

      self.broker_call = 
         function(self,args)
            if not self.registry then
               self.registry = zcontext:socket(zmq.REQ) 
               local broker_url = 'tcp://'..config.broker.ip..':'..config.broker.registry_port
               self.registry:connect(broker_url)
            end
            for i,arg in ipairs(args) do          
               self.registry:send(arg,zSNDMORE)
            end
            self.registry:send(config.name)
            local resp = self.registry:recv()
            if self.registry:getopt(zRCVMORE) > 0 then
               error('broker call "'..tconcat(args,',')..'" failed:'..self.registry:recv())
            else
               return resp
            end
         end

      self.listen_init = 
         function(self)        
            assert(not self.listen)
            self.listen = zcontext:socket(zmq.PULL)
            self.listen_port = self:broker_call{'listen_open'}
            local url = 'tcp://'..config.broker.ip..':'..self.listen_port
            self.listen:connect(url)
            self.listen_callbacks = {}          
         end

      self.listen_add = 
         function(self,expr,func)
            assert(expr,func)
            if not self.listen then
               self:listen_init()
            end
            self:broker_call{'listen_add',self.listen_port,expr}
            self.listen_callbacks[expr] = func
         end

      self.listen_remove = 
         function(self,expr)
            assert(expr and self.listen_callbacks and self.listen_callbacks[expr])
            self:broker_call{'listen_remove',self.listen_port,expr}
            self.listen_callbacks[expr] = nil            
         end

      self.replier_init = 
         function(self)
            assert(not self.rep)
            self.rep = zcontext:socket(zmq.REP)
            self.rep_port = self:broker_call{'replier_open'}
            local url = 'tcp://'..config.broker.ip..':'..self.rep_port
            self.rep:connect(url)
            self.reply_callbacks = {}
         end
      
      self.replier_add = 
         function(self,expr,func,async)
            assert(expr,func) -- async is optional
            if not self.rep then
               self:replier_init()
            end
            self:broker_call{'replier_add',self.rep_port,expr}
            self.reply_callbacks[expr] = {
               func = func,
               async = async
            }            
         end

      self.replier_remove = 
         function(self,expr)
            assert(expr and self.reply_callbacks and self.reply_callbacks[expr])
            self:broker_call{'replier_remove',self.rep_port,expr}
            self.reply_callbacks[expr] = nil            
         end

      -- upvalues for dispatch_request and dispatch_notifications
      local zmethods = zutil.zmq_methods(zcontext)
      local recv = zmethods.recv
      local send = zmethods.send
      local getopt = zmethods.getopt
      
      local reply_callbacks
      local rep
      self.dispatch_request = 
         function()
            rep = rep or self.rep
            reply_callbacks = reply_callbacks or self.reply_callbacks
            local more 
            repeat
               local expr = recv(rep)
               local method = recv(rep)
               local arguments = recv(rep)        
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
               local cb = reply_callbacks[expr]
               if cb then
                  if cb.async then
                     result = {pcall(cb.func,
                                     method,
                                     on_success,
                                     on_error,
                                     unserialize_args(arguments))}
                  else
                     result = {pcall(cb.func,
                                     method,
                                     unserialize_args(arguments))}
                     if result[1] then 
                        on_success(unpack(result,2))
                        return
                     end
                  end
                  if not result[1] then 
                     on_error(result[2])
                  end         
               else
                  on_error('method '..method..' not found')
               end
               more = getopt(rep,zRCVMORE) > 0
            until not more
         end

      local listen
      local listen_callbacks
      self.dispatch_notifications = 
         function()
            listen = listen or self.listen
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
                     log('dispatch_notifications callback failed',expr,err)
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
               self:broker_call{'listen_close',self.listen_port}
            end
            if self.rep then
               self:broker_call{'replier_close',self.rep_port}
            end
            if self.notify_sock then 
               self.notify_sock:close() 
            end
            if self.listen then 
               self.listen:close() 
            end
            if self.rep then 
               self.rep:close() 
            end
            if self.registry then 
               self.registry:close() 
            end
            if self.rpc_sock then 
               self.rpc_sock:close()
            end
         end

      self.reply_io = 
         function(self)
            if not self.rep then self:replier_init() end
            return zutil.add_read_io(self.rep,self.dispatch_request)
         end
      
      self.listen_io = 
         function(self)
            if not self.listen then self:listen_init() end
            return zutil.add_read_io(self.listen,self.dispatch_notifications)
         end

      self.call = 
         function(self,method,...)
            assert(method)
            if not self.rpc_sock then
               self.rpc_sock = zcontext:socket(zmq.REQ)
               local url = 'tcp://'..config.broker.ip..':'..config.broker.rpc_port
               self.rpc_sock:connect(url)
            end
            local sock = self.rpc_sock
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

      self.call_async = 
         function(self,method,on_success,on_error,...)
            assert(method)
            if not self.rpc_sock then
               self.rpc_sock = zcontext:socket(zmq.REQ)
               local url = 'tcp://'..config.broker.ip..':'..config.broker.rpc_port
               self.rpc_sock:connect(url)
            end
            local sock = self.rpc_sock
            sock:send(method,zSNDMORE)
            sock:send(serialize_args(...))
            -- this recv NOBLOCK is absolutely required! 
            -- if left out, read_io will never be triggered!
            local resp = sock:recv(zmq.NOBLOCK)
            local dispatch_response = 
               function()
                  if sock:getopt(zRCVMORE) > 0 then
                     local err = sock:recv()
                     if sock:getopt(zRCVMORE) > 0 then            
                        local msg = sock:recv()
                        if on_error then
                           on_error(make_zerr(err,msg),2)
                        end
                     else
                        if on_error then
                           on_error(unserialize_err(err),2)
                        end
                     end
                  end
                  if on_success then
                     on_success(unserialize_result(resp))
                  end
               end
            if resp then
               dispatch_response()
            end
            zutil.add_read_io(
               self.rpc_sock,
               function(loop,io)
                  io:stop(loop)
                  zutil.remove_read_io(sock)      
                  resp = sock:recv()
                  dispatch_response()                  
               end):start(self.ev_loop)
         end

      self.loop = 
         function(self,options)
            local options = options or {}
            local listen_io = self:listen_io()
            local reply_io = self:reply_io()            
            local loop = self.ev_loop
            local SIGHUP = 1
            local SIGINT = 2
            local SIGKILL = 9
            local SIGTERM = 15	
            local quit = 
               function()
                  if options.exit then
                     options.exit()
                  end
                  if listen_io then listen_io:stop(loop) end
                  if reply_io then reply_io:stop(loop) end
                  if options.ios then
                     for _,io in ipairs(options.ios) do
                        io:stop(loop)
                     end
                  end
                  self:close()
                  zcontext:term()
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
            self.ev_loop:unloop()
         end
      
      return self
   end

return {
   new = new
}
