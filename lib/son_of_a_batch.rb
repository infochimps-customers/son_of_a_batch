require 'gorillib/numeric/clamp'
require 'gorillib/hash/slice'

module LogJammin
  FIBER_IDXS = {}

  def fiber_idx
    FIBER_IDXS[Fiber.current.object_id] ||= "fiber_#{FIBER_IDXS.length}"
  end

  def logline env, run_id, indent, *segs
    env.logger.debug( [fiber_idx, object_id, run_id, " "*indent+'>', segs].flatten.join("\t") )
  end
end

class BatchIterator < EM::Synchrony::Iterator
  include LogJammin
  attr_reader :requests, :responses

  DEFAULT_CONCURRENCY = 100
  HTTP_OPTIONS = { :connect_timeout => 1.0 }

  def initialize env, queries, options={}
    @env          = env
    @batch_id     = options[:batch_id] || object_id
    @show_stats   = options.delete(:show_stats)
    #
    @http_options = HTTP_OPTIONS.dup
    @http_options[:inactivity_timeout] = options[:timeout].to_f.clamp(2.0, 10.0)
    @http_options[:connect_timeout   ] = @http_options[:connect_timeout].clamp(nil, options[:timeout].to_f)
    @http_timeout = @http_options[:inactivity_timeout] + @http_options[:connect_timeout]
    concurrency   = options[:concurrency] || DEFAULT_CONCURRENCY

    @results_back = {}
    @errors_back  = {}
    super queries, concurrency
  end

  def perform
    EM.synchrony do
      logline @env, @batch_id, 1, "sync'y", "beg"

      EM.next_tick{ response_preamble }

      each(
        proc{|(req_id, url), iter|
          req = EM::HttpRequest.new(url, @http_options).aget
          req.callback{ handle_result(req_id, req) ; iter.next }
          req.errback{  handle_error(req_id, req)  ; iter.next }
          req.timeout @http_timeout + 0.25

          logline @env, @batch_id, 3, "request", "built", req_id, req.req.uri.query, 'timeout', [@http_options, @http_timeout]
        }, proc{
          response_coda
          @env.chunked_stream_close
        }
        )
      logline @env, @batch_id, 1, "sync'y", "end"
    end
  end

protected

  def response_preamble
  end

  def handle_result req_id, req
    @results_back[req_id] = {
      :status => req.response_header.http_status,
      :body => req.response.to_s.chomp,
      :headers => req.response_header
    }
  end

  def handle_error req_id, req
    err = req.error.to_s.empty? ? 'request error' : req.error
    @errors_back[req_id]  = { :error => err }
  end

  def stats
    {
      :took               => (Time.now.to_f - @env[:start_time]).round(3),
      :queries            => (@results_back.length + @errors_back.length),
      :concurrency        => concurrency,
      :inactivity_timeout => @http_options[:inactivity_timeout],
      :connect_timeout    => @http_options[:connect_timeout],
    }
  end

  def response_coda
  end
end

class JsonBatchIterator < BatchIterator
  def initialize env, queries, options={}
    @sep        = options.delete(:sep) || ""
    super
  end

  def response_preamble
    super
    @seen_first_result = false
    send "{"
    send %Q<"results":\{>, false
  end

  def handle_result req_id, req
    super
    key    = JSON.generate(req_id.to_s)
    body   = JSON.generate(@results_back[req_id].slice(:status, :body))
    send( @seen_first_result ? "," : "") ; @seen_first_result = true
    send key, ":", body, false
  end

  def handle_error req_id, req
    super
  end

  def response_coda
    super
    send ''
    send '},' # end results hash
    send '"stats":',  JSON.generate(stats), ',' if @show_stats
    send '"errors":', JSON.generate(@errors_back)
    send '}'
  end

  def send *parts
    sep = @sep
    if parts.last == false then sep = '' ; parts.pop ; end
    @env.chunked_stream_send(parts.join + sep)
  end
end

class TsvBatchIterator < BatchIterator
  def handle_result req_id, req
    super
    body = req.response.to_s.gsub(/[\r\n\t]+/, "")
    send_tsv '_r', req_id, req.response_header.http_status, body
  end

  def handle_error req_id, req
    super
    send_tsv '_e', req_id, "", %Q{{"error":'#{req.error}'}}
  end

  def response_coda
    super
    send_tsv '_s',  '', '', JSON.generate(stats) if @show_stats
  end

  def send_tsv *args
    @env.chunked_stream_send("#{args.join("\t")}\n")
  end
end

module LoggingIterator
  def perform *args
    logline @env, @batch_id, 1, "perform", "beg"
    super
    logline @env, @batch_id, 1, "perform", "end"
  end

  def response_preamble
    logline @env, @batch_id, 2, "resp", "preamb"
    super
  end

  def handle_result req_id, req
    logline @env, @batch_id, 3, "request", "success", req_id, req.req.uri.query
    super
  end

  def handle_error req_id, req
    logline @env, @batch_id, 3, "request", "error", req_id, req.req.uri.query
    super
  end

  def response_coda
    logline @env, @batch_id, 2, "resp", "coda"
    super
  end
end
