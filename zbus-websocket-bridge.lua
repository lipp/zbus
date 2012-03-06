local cjson = require'cjson'
local tinsert = table.insert
local ev = require'ev'
local websockets = require'websockets'
local zbus = require'zbus'
local file_dir = arg[1] or './'
local ws_ios = {}
local context = nil
local zbus = require'zbus'
local zbus_config = require'zbus.json'
zbus_config.name = 'jet.websocket'
zbus_config.ev_loop = ev.Loop.default
zbus_config.exit = 
   function()
      for fd,io in pairs(ws_ios) do
	 io:stop(ev.Loop.default)
      end
      context:destroy()
   end
local zm = zbus.member(zbus_config)

context = websockets.context{
   port = arg[2] or 8002,
   on_add_fd = 
      function(fd)	
	 local io = ev.IO.new(
	    function()
	       context:service(0)
	    end,fd,ev.READ)
	 ws_ios[fd] = io
	 io:start(ev.Loop.default)
      end,
   on_del_fd = 
      function(fd)
	 ws_ios[fd]:stop(ev.Loop.default)
	 ws_ios[fd] = nil
      end,
   on_http = 
      function(ws,uri)
	 if uri == '/favicon.ico' then
	    ws:serve_http_file(file_dir..'favicon.ico','image/x-icon')
	 elseif uri == '/' or uri == '/index.html' then
	    ws:serve_http_file(file_dir..'index.html','text/html')
	 else
	    local content_type
	    if uri:match('css$') then
	       content_type = 'text/css'
	    elseif uri:match('less$') then
	       content_type = 'text/less'
	    elseif uri:match('html$') then
	       content_type = 'text/html'
	    elseif uri:match('js$') then
	       content_type = 'text/javascript'
	    end
	    assert(content_type,'unknown content type:'..uri)
	    ws:serve_http_file(file_dir..uri,content_type)
	 end
      end,
   protocols = {
      ['zbus-call'] =
         function(ws)	  
            ws:on_receive(
      	       function(ws,data)
		  local req = cjson.decode(data)
		  local resp = {id=req.id}
		  local result = {pcall(zm.call,zm,req.method,unpack(req.params))}
		  if result[1] then 
		     table.remove(result,1);
		     resp.result = result
		  else
		     resp.error = result[2]
		  end
      		  ws:write(cjson.encode(resp),websockets.WRITE_TEXT)
      	       end)
         end,
      ['zbus-notification'] =
         function(ws)
	    ws:on_broadcast(websockets.WRITE_TEXT)
         end
   }
}

local notifications = {}

zm:listen_add(
   '.*',
   function(topic,more,...)
      tinsert(notifications,{
		 topic = topic,
		 data = {...}
	      })
      if not more then
	 context:broadcast('zbus-notification',
			   cjson.encode(notifications))
	 notifications = {}
      end
   end)

zm:loop()


