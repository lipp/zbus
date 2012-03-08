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
local tconcat = table.concat
local tinsert = table.insert
local smatch = string.match

module('zbus')

-- default configuration
local default_broker_port = 33329
local default_rpc_port = 33325
local default_notify_port = 33328
local default_ip = '127.0.0.1'
local url_pool_default_interface = '127.0.0.1'
local url_pool_default_port_min = 33430
local url_pool_default_port_max = 33500

local zcontext = zmq.init(1)
local zIN = zmq.POLLIN
local zINOUT = zmq.POLLIN + zmq.POLLOUT
local zSNDMORE = zmq.SNDMORE
local zRCVMORE = zmq.RCVMORE
local zEVENTS = zmq.EVENTS

local empty = 
   function(tab)
      return not pairs(tab)(tab)
   end

local identity = 
  function(a)
    assert(type(a)=='string')
    return a
  end

local default_make_zerr = 
  function(code,msg)
    return 'zbus error:'..msg..'('..code..')'
  end

member = 
  function(config)
    local config = config or {}
    local mname = config.name or 'unknown_member'
    local log = config.log or function() end
    local self = {}        
    local serialize_args = config.serialize_args or identity
    local serialize_result = config.serialize_result or identity
    local serialize_err = config.serialize_err or identity
    local make_zerr = config.make_zerr or default_make_zerr
    local unserialize_args = config.unserialize_args or identity
    local unserialize_result = config.unserialize_result or identity
    local unserialize_err = config.unserialize_err or identity
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
        local recv = rep.recv
        local send = rep.send
        local expr = recv(rep)
        local method = recv(rep)
        local arguments = recv(rep)
	assert(self.reply_callbacks[expr])
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
        if self.reply_callbacks[expr].async then
          result = {pcall(self.reply_callbacks[expr].func,
                          method,
                          on_success,
                          on_error,
                          unserialize_args(arguments))}
        else
          result = {pcall(self.reply_callbacks[expr].func,
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
    self.handle_notify = 
      function()
	 local listen = self.listen
        local recv = listen.recv
        local more
	 repeat 
	    local expr = recv(listen)
	    local topic = recv(listen)
	    local arguments = recv(listen)
--	    assert(self.listen_callbacks[expr])
	    more = listen:getopt(zRCVMORE) > 0
	    self.listen_callbacks[expr](topic,more,unserialize_args(arguments))
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
	    local notify_url = 'tcp://'..(config.notify_ip or default_ip)..':'..(config.notify_port or default_notify_port)
	    self.notify_sock:connect(notify_url)    
            nsock = self.notify_sock
	 end
         local send = nsock.send
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



   self.rpc_socks = {}
   self.rpc_local_url = 'tcp://'..(config.rpc_ip or default_ip)..':'..(config.rpc_port or default_rpc_port)
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

   local zmq_init_msg = zmq.zmq_msg_t.init
   local zmq_copy_msg = 
      function(msg)
	 local cmsg = zmq_init_msg()
	 cmsg:copy(msg)
	 return cmsg
      end
   
    self.new_replier = 
      function(self,url)
          local replier = {}
          replier.exps = {}
          replier.dealer = zcontext:socket(zmq.XREQ)
          replier.dealer:connect(url)
          local dealer = replier.dealer
          local router = self.router
          local getopt = dealer.getopt
          local recv_msg = dealer.recv_msg
          local send_msg = router.send_msg
          local part_msg = zmq_init_msg() 
          self.poller:add(
            replier.dealer,zmq.POLLIN,
            function()
              local more              
              repeat
                recv_msg(dealer,part_msg)
                more = getopt(dealer,zRCVMORE) > 0
                send_msg(router,part_msg,more and zSNDMORE or 0)
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

   local xid_msg = zmq_init_msg()
   local empty_msg = zmq_init_msg()
   local method_msg = zmq_init_msg()
   local arg_msg = zmq_init_msg()

   local send_error = 
      function(router,xid_msg,err_id,err)
        local send = router.send
	   router:send_msg(xid_msg,zSNDMORE)
	   send(router,'',zSNDMORE)
	   send(router,'',zSNDMORE)
	   send(router,err_id,zSNDMORE)        
	   send(router,err,0)                	   
	end

    local router = self.router
    local rrecv_msg = router.recv_msg
    local rgetopt = router.getopt
    local forward_rpc =
      function()
	 rrecv_msg(router,xid_msg)
	 if rgetopt(router,zRCVMORE) < 1 then
	    send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_EMPTY_PART')
	 end
	 rrecv_msg(router,empty_msg)
	 assert(empty_msg:size()==0)
	 if rgetopt(router,zRCVMORE) < 1 then
	    send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_METHOD')
	 end
        rrecv_msg(router,method_msg)
        if rgetopt(router,zRCVMORE) < 1 then
	   send_error(router,xid_msg:data(),'ERR_INVALID_MESSAGE','NO_ARG')
	end
        rrecv_msg(router,arg_msg)        
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
           local send_msg = dealer.send_msg
          send_msg(dealer,xid_msg,zSNDMORE)
          send_msg(dealer,empty_msg,zSNDMORE)
          dealer:send(matched_exp,zSNDMORE)
          send_msg(dealer,method_msg,zSNDMORE)
          send_msg(dealer,arg_msg,0)          
        end
      end  

   local data_msg = zmq_init_msg()
   local topic_msg = zmq_init_msg()
   local notify = self.notify
   local nrecv_msg = notify.recv_msg
   local ngetopt = notify.getopt
   local listeners = self.listeners   
   local forward_notify = 
      function()
	 local todos = {}
	 repeat
	    nrecv_msg(notify,topic_msg)	   
	    if ngetopt(notify,zRCVMORE) < 1 then
	       log('notify','invalid message','no_data')
	       break
	    end
	    local topic = tostring(topic_msg)
	    nrecv_msg(notify,data_msg)
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
	 until ngetopt(notify,zRCVMORE) <= 0
	 for listener,notifications in pairs(todos) do
	    local len = #notifications
            local lpush = listener.push
	    for i,notification in ipairs(notifications) do
	       local option = zSNDMORE
	       if i == len then
		  option = 0
	       end
	       lpush:send(notification[1],zSNDMORE)
	       lpush:send_msg(notification[2],zSNDMORE)
	       lpush:send_msg(notification[3],option)
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


