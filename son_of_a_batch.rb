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

  QUERIES = [1.0, 1.5, 2.0, 0.5, 1.0, 0.25]

  def recent_latency
    Goliath::Plugin::Latency.recent_latency if defined?(Goliath::Plugin::Latency)
  end

  def response(env)
    case env['PATH_INFO']
    when '/'         then return [200, {}, haml(:root)]
    when '/debug'    then return [200, {}, haml(:debug)]
    when '/get'      then :pass
    else                  raise Goliath::Validation::NotFoundError
    end

    start = Time.now.utc.to_f
    env.logger.debug "iterator #{start}: starting target requests"

    EM::Synchrony::Iterator.new(QUERIES.each_with_index.to_a, TARGET_CONCURRENCY).each(
      proc{|(delay, idx), iter|

        env.logger.debug "iterator #{start} [#{delay}, #{idx}]: requesting target"
        c = EM::HttpRequest.new("#{TARGET_URL_BASE}?delay=#{delay}").aget
        env.logger.debug "iterator #{start} [#{delay}, #{idx}]: requested target"

        c.callback do
          env.chunked_stream_send(c.response+"\n")
          env.logger.debug "iterator #{start} [#{delay}, #{idx}]: target iter.next"
          iter.next
        end

        env.logger.debug "iterator #{start} [#{delay}, #{idx}]: end target request iter"

      }, proc{|responses|
        env.logger.debug "iterator #{start}: closing stream"
        env.chunked_stream_close
      })

    # results = { :results => {} , :errors => {} }
    # data.responses.each do |resp_type, resp_hsh|
    #   resp_type = (resp_type == :callbacks) ? :results : :errors
    #   resp_hsh.each do |req,resp|
    #     parsed = JSON.parse(resp.response) rescue resp.response
    #     results[:results][req] = [resp.response_header.http_status, resp.response_header.to_hash, parsed]
    #   end
    # end

    env.logger.debug "timer #{start}: after fetch"

    chunked_streaming_response 200, {'X-Responder' => self.class.to_s }
  end
end


