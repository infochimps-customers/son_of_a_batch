#!/usr/bin/env ruby
$:<< './lib'

require 'boot'

require 'goliath'
require 'em-synchrony/em-http'

class Simple < Goliath::API
  use Goliath::Rack::Params             # parse query & body params
  # plugin Goliath::Plugin::Latency       # ask eventmachine reactor to track its latency

  QUERIES = [ 1.0, 14.5, 2.5, 0.5, 4.0, 0.25, 5.0, 9.5, 2.5, 0.5, 2.3, 2.5 ]
  TARGET_CONCURRENCY   = QUERIES.length

  def response(env)
    batch_id = "%7.04f" % (env[:start_time].to_f - 100*(env[:start_time].to_f.to_i / 100))

    env.logger.debug "req #{object_id} @#{batch_id}: constructing request group"
    BatchIterator.new(env, batch_id, QUERIES.each_with_index.to_a, TARGET_CONCURRENCY).perform
    env.logger.debug "req #{object_id} @#{batch_id}: constructed request group"
    chunked_streaming_response(200, {'X-Responder' => self.class.to_s, })
  end
end


class BatchIterator < EM::Synchrony::Iterator

  TARGET_URL_BASE = "http://localhost:9002/meta/http/sleepy.json"
  HTTP_REQUEST_OPTIONS = { :connect_timeout => 1.0, :inactivity_timeout => 1.2 }

  attr_reader :requests, :responses

  def initialize env, batch_id, *args
    @env       = env
    @batch_id  = batch_id
    @requests  = []
    @responses = {:results => {}, :errors => {}}
    super *args
  end

  def handle_result req_id, req
    @env.chunked_stream_send([req_id, "\t", req.response_header.http_status, "\t", req.response, "\n"].join)
  end

  def handle_error req_id, req
    @env.chunked_stream_send([req_id, "\t", "", "\t", %Q{{"error":'#{req.error}'}}, "\n"].join)
  end

  def logline indent, seg
    @env.logger.debug( "req %s @%s: %s%s" % [object_id, @batch_id, " "*indent, seg] )
  end

  def perform
    EM.synchrony do
      logline 0, "synchrony start"

      each(
        proc{|(delay, req_id), iter|
          req = EM::HttpRequest.new(TARGET_URL_BASE, HTTP_REQUEST_OPTIONS).aget(:query => { :delay => delay })
          req.callback{ handle_result(req_id, req) ; iter.next ; logline(4, "request #{[req_id, delay]}\tsuccess") }
          req.errback{  handle_error(req_id, req)  ; iter.next ; logline(4, "request #{[req_id, delay]}\terror")   }

          logline 4, "request #{[req_id, delay]}\tconstructed"
        }, proc{
          logline 2, "stream closing"
          @env.chunked_stream_close
        }
        )
      logline 0, "synchrony end"
    end
  end

end


