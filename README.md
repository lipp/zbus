# About

zbus is a message bus in Lua. Even if the broker (zbusd.lua) and the modules provided are written in Lua, zbus members could be written in any language with support for zeromq (and multi-part messages).

## Files

zbus consists of several parts: 
     - zbusd.lua: A Lua program, which acts as "message broker" or "router"
     - zbus.lua: The Lua module, which provides an API for being a zbus member (zbus.member)
     - zbus/json.lua: An optional Lua module, which defines JSON serialization methods

## Purpose

zbus is designed to allow:
     - inter-process method calls
     - inter-process notifications (publish/subscribe)

To achieve this, you have to become a zbus.member. zbus members can:
     - register callbacks to handle inter-process method calls.
     - register callbacks to handle notifications (publish/_subscribe_)
     - send notificiations (_publish_/subscribe)
     - call methods in another process
     - a extendable event loop

## Requirements

zbusd heavily relies on zeromq (2.1) and lua-zmq. To operate, 
`zbusd.lua` is required to run (like dbus-daemon)!

## Protocol

zbus defines a simple protocol based on zeromq *multi-part messages*.This allows zbusd.lua to effectively recognize (or simply forward):
     - method-urls
     - method-arguments
     - return-values
     - exceptions
     - notification-data

The zbus protocol itself is aware of any dataformat but provides a default implementation for JSON which allows a very convient zbus.
