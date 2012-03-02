local zmq = require'zmq'
local zpoller = require'zmq.poller'
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
local setmetatable = setmetatable
local cjson = require'cjson'
local os = require'os'
local print = print
local tconcat = table.concat

module('zbus')
local zcontext = zmq.init(1)
local default_broker_port = 33329
local default_rpc_port = 33325
local default_notify_port = 33328
local default_ip = '127.0.0.1'
local url_pool_default_interface = '127.0.0.1'
local url_pool_default_port_min = 33430
local url_pool_default_port_max = 33500
local zIN = zmq.POLLIN
local zINOUT = zmq.POLLIN + zmq.POLLOUT
local zSNDMORE = zmq.SNDMORE
local zRCVMORE = zmq.RCVMORE
local zEVENTS = zmq.EVENTS


local identity = 
  function(a)
    assert(type(a)=='string')
    return a
  end

local default_make_zerr = 
  function(code,msg)
    return 'zbus error:'..msg..'('..code..')'
  end


local log
member = 
  function(config)
    local config = config or {}
    local mname = config.name or 'unknown_member'
    local log = config.log or function() end
    local self = {}        
    self.serialize_args = config.serialize_args or identity
    self.serialize_result = config.serialize_result or identity
    self.serialize_err = config.serialize_err or identity
    self.make_zerr = config.make_zerr or default_make_zerr
    self.unserialize_args = config.unserialize_args or identity
    self.unserialize_result = config.unserialize_result or identity
    self.unserialize_err = config.unserialize_err or identity
    self.broker_call = 
      function(self,args)
        if not self.broker_reg then
          self.broker_reg = zcontext:socket(zmq.REQ) 
          local broker_url = 'tcp://'..(config.broker_ip or default_ip)..':'..(config.broker_port or default_broker_port)
          self.broker_reg:connect(broker_url)
        end
        for i,arg in ipairs(args) do          
            self.broker_reg:send(arg,zSNDMORE)
        end
        self.broker_reg:send(mname)
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
        self:broker_call{'listen_add',expr,self.listen_url}
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
        self:broker_call{'replier_add',expr,self.rep_url}
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
    self.handle_req = 
      function()        
        log('handle_req')
        local rep = self.rep
        local expr = rep:recv()
        local method = rep:recv()
        local arguments = rep:recv()
	assert(self.reply_callbacks[expr])
--        log('handle_req',expr,method,arguments,'async',self.reply_callbacks[expr].async)                
        local on_success = 
          function(...)
            rep:send(self.serialize_result(...))  
          end
        local on_error = 
          function(err)
            rep:send('',zSNDMORE)          
            rep:send(self.serialize_err(err))
          end        
        local result        
        if self.reply_callbacks[expr].async then
          result = {pcall(self.reply_callbacks[expr].func,
                          method,
                          on_success,
                          on_error,
                          self.unserialize_args(arguments))}
        else
          result = {pcall(self.reply_callbacks[expr].func,
                          method,
                          self.unserialize_args(arguments))}
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
    self.handle_notify = 
      function()
        local listen = self.listen
        local expr = listen:recv()
        local method = listen:recv()
        local arguments = listen:recv()
	assert(self.listen_callbacks[expr])
        self.listen_callbacks[expr](method,self.unserialize_args(arguments))
      end

    self.notify = 
      function(self,topic,...)
        if not self.notify_sock then
          self.notify_sock = zcontext:socket(zmq.PUSH)
          local notify_url = 'tcp://'..(config.notify_ip or default_ip)..':'..(config.notify_port or default_notify_port)
          self.notify_sock:connect(notify_url)    
        end
        self.notify_sock:send(topic,zSNDMORE)
        self.notify_sock:send(self.serialize_args(...),0)
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
        if self.rpc_sock then 
	   self.rpc_sock:close() 
	end
      end

    self.reply_io = 
      function(self)
        if not self.rep then self:replier_init() end
        return ev.IO.new(
          function(loop,io)
            io:stop(loop)
            while true do
              local events = self.rep:getopt(zEVENTS)
              if events == 0 then 
                io:start(loop)
                break 
              end
              if events == zIN or events == zINOUT then      
                self.handle_req()
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
            io:stop(loop)
            while true do
              local events = self.listen:getopt(zEVENTS)
              if events == 0 then 
                io:start(loop)
                break 
              end
              if events == zIN or events == zINOUT then      
                self.handle_notify()
              end
            end
          end,
          self.listen:getopt(zmq.FD),
          ev.READ)     
      end

    self.call = 
      function(self,method,...)
        if not self.rpc_sock then
          self.rpc_sock = zcontext:socket(zmq.REQ)
          local rpc_url = 'tcp://'..(config.rpc_ip or default_ip)..':'..(config.rpc_port or default_rpc_port)
          self.rpc_sock:connect(rpc_url)
        end
        self.rpc_sock:send(method,zSNDMORE)
        self.rpc_sock:send(self.serialize_args(...))
        local resp = self.rpc_sock:recv()
        if self.rpc_sock:getopt(zRCVMORE) > 0 then
          local err = self.rpc_sock:recv()
          if self.rpc_sock:getopt(zRCVMORE) > 0 then            
            local msg = self.rpc_sock:recv()
            error(self.make_zerr(err,msg),2)
          else
            error(self.unserialize_err(err),2)
          end
        end
        return self.unserialize_result(resp)
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
        if listen_io then log('LISTEN');listen_io:start(loop) end
        if reply_io then log('REPLY');reply_io:start(loop) end
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

broker = 
  function(config)
    local config = config or {}
    local self = {}
    local log = config.log or function() end 
    self.reg = zcontext:socket(zmq.REP)
    self.reg:bind('tcp://*:'..(config.broker_port or default_broker_port))
    self.router = zcontext:socket(zmq.XREP)
    log(self.router,self.reg)
    self.router:bind('tcp://*:'..(config.rpc_port or default_rpc_port))
    self.notify = zcontext:socket(zmq.PULL)
    local notify_url = 'tcp://'..(config.notify_ip or default_ip)..':'..(config.notify_port or default_notify_port)
    self.notify:bind(notify_url)
    self.poller = config.poller or zpoller(2)
    self.repliers = {}
    self.listeners = {}
    self.url_pool = url_pool(
       config.url_pool_interface or url_pool_default_interface,
       config.url_pool_port_min or url_pool_default_port_min,
       config.url_pool_port_min  or url_pool_default_port_max
    )

    self.new_replier = 
      function(self,url)
          local replier = {}
          replier.exps = {}
          replier.dealer = zcontext:socket(zmq.XREQ)
          replier.dealer:connect(url)
          local dealer = replier.dealer
          local router = self.router
          self.poller:add(
            replier.dealer,zmq.POLLIN,
            function()
              local more
              repeat
                local part = dealer:recv()
                more = dealer:getopt(zRCVMORE) > 0
                router:send(part,more and zSNDMORE or 0)
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
          if not self.repliers[replier_url] then
            self:new_replier(replier_url)
          end 
        end,
      replier_close = 
        function(replier_url)
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
        function(exp,replier_url)
          table.insert(self.repliers[replier_url].exps,exp)          
        end,
      replier_remove = 
        function(replier_url,exp)
          local rep = self.repliers[replier_url]
          if rep then
            for i=1,#rep.exps do
              if rep.exps[i] == exp then
                table.remove(rep.exps,i)
              end
            end
          end
        end,
      listen_open = 
        function(listen_url)
          if not self.listeners[listen_url] then
            self:new_listener(listen_url)
          end          
        end,
      listen_close = 
        function(listen_url)
          if self.listeners[listen_url] then
            self.listeners[listen_url].push:close()
            self.listeners[listen_url] = nil
          end
        end,
      listen_add = 
        function(exp,listen_url)
          table.insert(self.listeners[listen_url].exps,exp)          
        end,
      listen_remove = 
        function(listen_url,exp)
          local listener = self.listeners[listen_url]
          if listener then
            for i=1,#listener.exps do
              if listener.exps[i] == exp then
                table.remove(listener.exps,i)
              end
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

   local send_error = 
      function(router,xid,err_id,err)
	   router:send(xid,zSNDMORE)
	   router:send('',zSNDMORE)
	   router:send('',zSNDMORE)
	   router:send(err_id,zSNDMORE)        
	   router:send(err,0)                	   
      end
    
    local forward_rpc =
      function()
        local router = self.router
        local xid = router:recv()	
        if router:getopt(zRCVMORE) < 1 then
	   send_error(router,xid,'ERR_INVALID_MESSAGE','NO_EMPTY_PART')
	end
        assert(router:recv()=='')
        if router:getopt(zRCVMORE) < 1 then
	   send_error(router,xid,'ERR_INVALID_MESSAGE','NO_METHOD')
	end
        local method = router:recv()
        if router:getopt(zRCVMORE) < 1 then
	   send_error(router,xid,'ERR_INVALID_MESSAGE','NO_ARG')
	end
        local arg = router:recv()        
        log('rpc',method,arg)
        local dealer,matched_exp
        local err,err_id
        for url,replier in pairs(self.repliers) do 
          for _,exp in pairs(replier.exps) do
            log('rpc','trying',exp,method)--,method:match(exp))
            if method:match(exp) then
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
	   send_error(router,xid,err_id,err)
        else          
          dealer:send(xid,zSNDMORE)
          dealer:send('',zSNDMORE)
          dealer:send(matched_exp,zSNDMORE)
          dealer:send(method,zSNDMORE)
          dealer:send(arg,0)          
        end
      end  

    local forward_notify = 
      function()
        repeat
          local notify = self.notify
          local topic = notify:recv()
          if self.notify:getopt(zRCVMORE) < 1 then
	     log('notify','invalid message','no_data')
	  end
          local data = notify:recv()
          log('notify',topic,data)
          for url,listener in pairs(self.listeners) do
            for _,exp in pairs(listener.exps) do
              log('notify','trying',topic,data)
              if topic:match(exp) then
                log('notify','matched',topic,exp,url)
                listener.push:send(exp,zSNDMORE)
                listener.push:send(topic,zSNDMORE)
                listener.push:send(data,0)
              end
            end
          end
        until notify:getopt(zRCVMORE) <= 0
      end
    
    self.forward_rpc = forward_rpc
    self.dispatch_reg = dispatch_reg
    self.forward_notify = forward_notify
    self.loop = 
      function(self)
        self.poller:add(
          self.reg,zmq.POLLIN,
          dispatch_reg)
        self.poller:add(
          self.router,zmq.POLLIN,
          forward_rpc)
        self.poller:add(
          self.notify,zmq.POLLIN,
          forward_notify)
        self.poller:start()
      end
    return self
  end


