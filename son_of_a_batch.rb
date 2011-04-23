#!/usr/bin/env ruby
$:<< './lib'

# A simple dashboard for
#
# See
#   app/views                -- templates
#   public                   -- static files
#   config/son_of_a_batch.rb -- configuration
#

require 'boot'
require 'tilt'
require 'yajl/json_gem'

require 'goliath'
require 'goliath/rack/templates'
require 'goliath/plugins/latency'
require 'em-synchrony/em-http'
require 'rack/abstract_format'

class SonOfABatch < Goliath::API
  use Goliath::Rack::Params             # parse query & body params
  use Goliath::Rack::Formatters::JSON   # JSON output formatter
  use Goliath::Rack::Render             # auto-negotiate response format
  use Rack::AbstractFormat, 'application/json'

  include Goliath::Rack::Templates      # render templated files from ./views
  use(Rack::Static,                     # render static files from ./public
    :root => Goliath::Application.root_path("public"), :urls => ["/favicon.ico", '/stylesheets', '/javascripts', '/images'])

  # plugin Goliath::Plugin::Latency       # ask eventmachine reactor to track its latency

  TARGET_URL_BASE = "http://localhost:9002/meta/http/sleepy.json"

  TARGET_CONCURRENCY = 10
  MAX_TARGET_QUERIES = 100
  TIMEOUT            = 30

  QUERIES = [ 1.0, 1.5, 2.0, 0.5, 1.0, 0.25 ]

  def recent_latency
    Goliath::Plugin::Latency.recent_latency if defined?(Goliath::Plugin::Latency)
  end

  SEP = "\n"
  BATCH_OPEN    = ["{", SEP].join
  RESULTS_OPEN  = %Q<"results":\{#{SEP}>
  RESULTS_CLOSE = %Q<#{SEP}\}>
  BATCH_CLOSE   = [SEP, "}"].join

  def response(env)
    case env['PATH_INFO']
    when '/'         then return [200, {}, haml(:root)]
    when '/debug'    then return [200, {}, haml(:debug)]
    when '/get'      then :pass
    else                  raise Goliath::Validation::NotFoundError
    end

    start = Time.now.utc.to_f
    env.logger.debug "iterator #{start}: starting target requests"

    EM.synchrony do

      saved_responses   = {}
      seen_first_result = false

      EM.next_tick{ env.chunked_stream_send( [BATCH_OPEN, RESULTS_OPEN].join ) }

      EM::Synchrony::Iterator.new(QUERIES.each_with_index.to_a, TARGET_CONCURRENCY).each(
        proc{|(delay, idx), iter|
          env.logger.debug "iterator #{start} [#{delay}, #{idx}]: requesting target"
          c = EM::HttpRequest.new("#{TARGET_URL_BASE}?delay=#{delay}").aget
          env.logger.debug "iterator #{start} [#{delay}, #{idx}]: requested target"

          c.callback do
            saved_responses[idx] = [c.response_header.http_status, c.response_header.to_hash, c.response]
            body = JSON.generate({ :status => c.response_header.http_status, :body => c.response })
            env.chunked_stream_send([ (seen_first_result ? "," : ""), %Q{"#{idx}":}, body, SEP ].join)
            env.logger.debug "iterator #{start} [#{delay}, #{idx}]: target iter.next"
            seen_first_result ||= true
            iter.next
          end

          env.logger.debug "iterator #{start} [#{delay}, #{idx}]: end target request iter"

        }, proc{

          # p saved_responses
          # env.chunked_stream_send JSON.pretty_generate(saved_responses)
          # env.chunked_stream_send %Q<"_done":{"completed_in":#{Time.now.utc.to_f - start}}>
          env.chunked_stream_send [RESULTS_CLOSE, ",", SEP, %Q{"errors":{}}].join
          env.chunked_stream_send BATCH_CLOSE
          env.logger.debug "iterator #{start}: closing stream"
          env.chunked_stream_close
        })

      env.logger.debug "timer #{start}: end of synchrony block"
    end

    # results = { :results => {} , :errors => {} }
    # data.responses.each do |resp_type, resp_hsh|
    #   resp_type = (resp_type == :callbacks) ? :results : :errors
    #   resp_hsh.each do |req,resp|
    #     parsed = JSON.parse(resp.response) rescue resp.response
    #     results[:results][req] = [resp.response_header.http_status, resp.response_header.to_hash, parsed]
    #   end
    # end

    env.logger.debug "timer #{start}: after constructing response"

    chunked_streaming_response(200, {'X-Responder' => self.class.to_s })
  end
end


