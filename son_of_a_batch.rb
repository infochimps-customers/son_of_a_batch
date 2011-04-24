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
require 'gorillib'
require 'gorillib/string/human'
require 'tilt'
require 'yajl/json_gem'

require 'goliath'
require 'goliath/rack/templates'
require 'goliath/plugins/latency'
require 'em-synchrony/em-http'
require 'rack/abstract_format'

# def h(text); Rack::Utils.escape_html text end


class SonOfABatch < Goliath::API
  use Goliath::Rack::Params             # parse query & body params
  use Goliath::Rack::Formatters::JSON   # JSON output formatter
  use Goliath::Rack::Render             # auto-negotiate response format
  use(Rack::Static,                     # render static files from ./public
    :root => Goliath::Application.root_path("public"), :urls => ["/favicon.ico", '/stylesheets', '/javascripts', '/images'])
  use Rack::AbstractFormat, 'text/html'

  include Goliath::Rack::Templates      # render templated files from ./views
  # plugin Goliath::Plugin::Latency       # ask eventmachine reactor to track its latency

  QUERIES = [ 1.0, 14.5, 2.5, 0.5, 4.0, 0.25, 5.0, 9.5, 2.5, 0.5, 1.0, 0.25 ]
  TARGET_CONCURRENCY   = QUERIES.length

  def recent_latency
    Goliath::Plugin::Latency.recent_latency if defined?(Goliath::Plugin::Latency)
  end

  def response(env)
    batch_id = "%7.04f" % (env[:start_time].to_f - 100*(env[:start_time].to_f.to_i / 100))
    case env['PATH_INFO']
    when '/'         then return [200, {}, haml(:root)]
    when '/debug'    then return [200, {}, haml(:debug)]
    when '/joke'     then return [200, {}, haml(:joke)]
    when '/get'      then :pass
    else                  raise Goliath::Validation::NotFoundError
    end

    env.logger.debug "req #{object_id} @#{batch_id}: constructing request group"
    BatchIterator.new(env, batch_id, QUERIES.each_with_index.to_a, TARGET_CONCURRENCY).perform
    env.logger.debug "req #{object_id} @#{batch_id}: constructed request group"
    chunked_streaming_response(200, {'X-Responder' => self.class.to_s, })
  end
end


class BatchIterator < EM::Synchrony::Iterator

  TARGET_URL_BASE = "http://localhost:9002/meta/http/sleepy.json"
  HTTP_REQUEST_OPTIONS = { :connect_timeout => 1.0, :inactivity_timeout => 1.4 }

  attr_reader :requests, :responses

  def initialize env, batch_id, *args
    @env = env
    @batch_id = batch_id
    @requests = []
    @responses = {:results => {}, :errors => {}}
    @seen_first_result = false
    super *args
  end

  def handle_result req_id, req
    @responses[:results][req_id] = { :status => req.response_header.http_status, :body => req.response }
    sep  = @seen_first_result ? ",#{SEP}" : ""
    key  = %Q{"#{req_id}":}
    body = JSON.generate(@responses[:results][req_id])
    @env.chunked_stream_send([ sep, key, body ].join)
  end

  def handle_error req_id, req
    err = req.error.blank? ? 'request error' : req.error
    @responses[:errors][req_id] = { :error => err }
    p req.error
  end

  def perform
    EM.synchrony do
      @env.logger.debug "req #{object_id} @#{@batch_id}:   synchrony block start"

    EM.next_tick{ beg_batch ; beg_results_block }
    each(
      proc{|(delay, req_id), iter|
        @env.logger.debug "req #{object_id} @#{@batch_id}:     request [#{req_id}, #{delay}]\tconstructing"
        req = EM::HttpRequest.new(TARGET_URL_BASE, HTTP_REQUEST_OPTIONS).aget(:query => { :delay => delay })

        req.callback do
          @env.logger.debug "req #{object_id} @#{@batch_id}:     request [#{req_id}, #{delay}]:\t  callback"
          handle_result(req_id, req)
          @seen_first_result ||= true
          @env.logger.debug "req #{object_id} @#{@batch_id}:     request [#{req_id}, #{delay}]:\t  iter.next"
          iter.next
        end

        req.errback do
          @env.logger.debug "req #{object_id} @#{@batch_id}:     request [#{req_id}, #{delay}]:\t  errback"
          handle_error(req_id, req)
          @env.logger.debug "req #{object_id} @#{@batch_id}:     request [#{req_id}, #{delay}]:\t  iter.next"
          iter.next
        end

        @env.logger.debug "req #{object_id} @#{@batch_id}:     request [#{req_id}, #{delay}]\tconstructed"

      }, proc{
        end_results_block
        @env.chunked_stream_send [",", SEP].join
        @env.chunked_stream_send JSON.generate({:errors => responses[:errors], :completed_in => (Time.now.utc.to_f - @env[:start_time].to_f)})[1..-2]
        end_batch
        @env.logger.debug "req #{object_id} @#{@batch_id}:   closing stream"
        @env.chunked_stream_close
      }
      )
      @env.logger.debug "req #{object_id} @#{@batch_id}: synchrony block end"
    end
  end


  SEP = "\n"
  BEG_BATCH    = "{"
  BEG_RESULTS  = %Q<"results":\{>
  END_RESULTS = "}"
  END_BATCH   = "}"

  def beg_batch
    @env.chunked_stream_send( [BEG_BATCH, SEP].join )
  end

  def beg_results_block
    @env.chunked_stream_send( [BEG_RESULTS, SEP].join )
  end

  def end_results_block
    @env.chunked_stream_send [SEP, END_RESULTS].join
  end

  def end_batch
    @env.chunked_stream_send( [SEP, END_BATCH].join )
  end

end


