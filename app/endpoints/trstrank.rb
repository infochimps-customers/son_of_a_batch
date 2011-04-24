#!/usr/bin/env ruby
$:<< './lib'

require 'boot'

require 'goliath'
require 'em-synchrony/em-http'

THINGIES = {}

class Trstrank < Goliath::API
  use Goliath::Rack::Params             # parse query & body params
  # plugin Goliath::Plugin::Latency       # ask eventmachine reactor to track its latency

  QUERIES = %w[ mrflip infochimps justinbieber thedatachef damon cheaptweet austinonrails aseever  mrflip infochimps justinbieber thedatachef damon cheaptweet austinonrails aseever ]
  TARGET_CONCURRENCY   = QUERIES.length

  def response(env)
    batch_id = "%7.04f" % (env[:start_time].to_f - 100*(env[:start_time].to_f.to_i / 100))

    env.logger.debug "req #{Fiber.current.object_id} #{object_id} @#{batch_id}: constructing request group"
    BatchIterator.new(env, batch_id, QUERIES.each_with_index.to_a, TARGET_CONCURRENCY).perform

    env.logger.debug "req #{Fiber.current.object_id} #{object_id} @#{batch_id}: constructed request group"
    chunked_streaming_response(200, {'X-Responder' => self.class.to_s, })
  end
end


class BatchIterator < EM::Synchrony::Iterator

  TARGET_URL_BASE = "http://api.infochimps.com/social/network/tw/influence/trstrank"
  HTTP_REQUEST_OPTIONS = { :connect_timeout => 0.5, :inactivity_timeout => 3.0 }

  attr_reader :requests, :responses

  def initialize env, batch_id, *args
    @env       = env
    @batch_id  = batch_id
    @requests  = []
    @responses = {:results => {}, :errors => {}}
    super *args
  end

  def handle_result req_id, req
    @env.chunked_stream_send([req_id, "\t", req.response_header.http_status, "\t", req.response.chomp, "\n"].join)
  end

  def handle_error req_id, req
    @env.chunked_stream_send([req_id, "\t", "", "\t", %Q{{"error":'#{req.error}'}}, "\n"].join)
  end

  def logline indent, seg
    THINGIES[Fiber.current.object_id] ||= "fiber_#{THINGIES.length}" ; thingy = THINGIES[Fiber.current.object_id]
    @env.logger.debug( "req %s %s %s @%s: %s%s" % [thingy, Fiber.current.object_id, object_id, @batch_id, " "*indent, seg] )
  end

  def perform
      logline 0, "perform start"
    EM.synchrony do
      logline 0, "synchrony start"

      each(
        proc{|(screen_name, req_id), iter|

          req_options = { :query => { :_apikey => @env.config[:infochimps_apikey], :screen_name => screen_name } }
          req = EM::HttpRequest.new(TARGET_URL_BASE, HTTP_REQUEST_OPTIONS).aget(req_options)
          req.callback{ handle_result(req_id, req) ; iter.next ; logline(4, "request #{[req_id, screen_name]}\tsuccess") }
          req.errback{  handle_error(req_id, req)  ; iter.next ; logline(4, "request #{[req_id, screen_name]}\terror")   }

          logline 4, "request #{[req_id, screen_name]}\tconstructed"
        }, proc{
          logline 2, "stream closing"
          @env.chunked_stream_close
        }
        )
      logline 0, "synchrony end"
    end
  end

end


