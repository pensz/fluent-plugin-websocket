# Copyright (C) 2013 IZAWA Tetsu (@moccos)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'em-websocket'
require 'thread'


class EventMachine::WebSocket::Connection
  attr_accessor :tags
end

module Fluent
  $lock = Mutex::new
  $channel = EM::Channel.new

  class WebSocketOutput < Fluent::Output
    Fluent::Plugin.register_output('websocket', self)
    config_param :use_msgpack, :bool, :default => false
    config_param :host, :string, :default => "0.0.0.0"
    config_param :port, :integer, :default => 8080
    config_param :add_time, :bool, :default => false
    config_param :add_tag, :bool, :default => true

    def configure(conf)
      super
      $thread = Thread.new do
      $log.trace "Started em-websocket thread."
      $log.info "WebSocket server #{@host}:#{@port} [msgpack: #{@use_msgpack}]"
      EM.run {
        EM::WebSocket.run(:host => @host, :port => @port) do |ws|
          ws.tags = []
          ws.onopen { |handshake|
            callback = @use_msgpack ? proc{|msg| 
              if ( ws.tags.include? msg[0]) then
                ws.send_binary(msg[1])   
              end
            } 
            : proc{|msg| 
              ws.send(msg)
            }

            $lock.synchronize do
              sid = $channel.subscribe callback
              $log.trace "WebSocket connection: ID " + sid.to_s
              ws.onclose {
                $log.trace "Connection closed: ID " + sid.to_s
                $lock.synchronize do
                  $channel.unsubscribe(sid)
                end
              }
            end

            ws.onmessage { |msg|
              if (msg == "reset")
                ws.tags.clear()
              elsif (msg.start_with? "add ")
                tag = msg[4..-1]

                if !ws.tags.include? tag
                  ws.tags << tag
                end
              elsif (msg.start_with? "del ")
                ws.tags.delete(msg[4..-1]) 
              end

              $log.info "msg from client: #{msg} final tag #{ws.tags}"
            }
          }
        end
      }
      end
    end

    def start
      super
    end

    def shutdown
      super
      EM.stop
      Thread::kill($thread)
      $log.trace "Killed em-websocket thread."
    end

    def emit(tag, es, chain)
      chain.next
      es.each {|time,record|
        data = [record]
        if (@add_time) then data.unshift(time) end
        if (@add_tag) then data.unshift(tag) end
        output = @use_msgpack ? data.to_msgpack : data.to_json
        $lock.synchronize do
          $channel.push [tag, output]
        end
      }
    end
  end
end
