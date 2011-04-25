require 'son_of_a_batch/logjammin'

class BatchIterator < EM::Synchrony::Iterator
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
    concurrency   = options[:concurrency] || DEFAULT_CONCURRENCY

    @results_back = {}
    @errors_back  = {}
    super queries, concurrency
  end

  def perform
    EM.synchrony do
      # first part of response
      EM.next_tick{ response_preamble }

      each(
        # called on each query
        proc{|(req_id, url), iter|
          req = EM::HttpRequest.new(url, @http_options).aget
          req.callback{ handle_result(req_id, req) ; iter.next }
          req.errback{  handle_error(req_id, req)  ; iter.next }
          # # a hack to work around a flaw in eventmachine -- fixed in latest git
          # req.timeout @http_timeout
          @env.logger.debug [@batch_id, "request", req_id, req.req.uri.query].join("\t")
        },
        # called at finish
        proc{
          response_coda
          @env.chunked_stream_close
        }
        )
    end
  end

protected

  # Called once before iterator starts, but after stream has initialized.
  # Use this for any initial portion of the payload
  def response_preamble
  end

  # Called on each unsuccessful response. Exactly ONE out of handle_result and
  # handle_error will be called for each request.
  #
  # @param [String]      req_id The arbitrary id for this request, chosen by the caller
  # @param [HttpRequest] req    The successful HttpRequest connection object
  #
  # Keep in mind that successful to HttpRequest means everything worked -- a 200
  # and an 404 and a 503 back from the client are all *successful* responses,
  # even if you don't think so.
  def handle_result req_id, req
    @results_back[req_id] = {
      :status => req.response_header.http_status,
      :body => req.response.to_s.chomp,
      :headers => req.response_header
    }
  end

  # Called on each unsuccessful response. Exactly ONE out of handle_result and
  # handle_error will be called for each request.
  #
  # @param [String]      req_id The arbitrary id for this request, chosen by the caller
  # @param [HttpRequest] req    The unsuccessful HttpRequest connection object
  #
  # Keep in mind that successful to HttpRequest means everything worked -- a 200
  # and an 404 and a 503 back from the client are all *successful* responses,
  # even if you don't think so.
  #
  # The error messages for HttpRequest are often (always?) blank -- a bug is
  # pending to fix this.
  def handle_error req_id, req
    err = req.error.to_s.empty? ? 'request error' : req.error
    @errors_back[req_id]  = { :error => err }
  end

  #
  # A helpful hash of statistics about the batch
  #
  def stats
    {
      :took               => (Time.now.to_f - @env[:start_time]).round(3),
      :queries            => (@results_back.length + @errors_back.length),
      :concurrency        => concurrency,
      :inactivity_timeout => @http_options[:inactivity_timeout],
      :connect_timeout    => @http_options[:connect_timeout],
    }
  end

  # Called after all responses have completed, before the stream is closed.
  def response_coda
  end
end

#
# Makes a JSON response in batches. The preamble opens a hash with field
# "results". Each successful response is delivered in a single line, as a single
# chunked-transfer chunk, mapping the req_id (as a json string) to a hash with
# the response http status code and the JSON-encoded body. NOTE: the body is
# JSON-encoded from whatever it was! If it was already JSON, you will need to
# call JSON.parse again on the body.
#
# Errors are accumulated as they roll in. After all requests complete, this
# dumps out a row with helpful stats, and the hash of errback results.
#
# With option[:sep], you can get first-order pretty-printing as shown (this also
# makes curl print the lines as they roll in):
#
#     {
#     "results":{
#     "13348":{"status":200,"body":"{\"trstrank\":4.9,\"user_id\":13348,\"screen_name\":\"Scobleizer\",\"tq\":99}"},
#     "18686296":{"status":200,"body":"{\"trstrank\":0.65,\"user_id\":18686296,\"screen_name\":\"bryanconnor\",\"tq\":99}"}
#     },
#     "stats":{"took":3.593,"queries":100,"concurrency":15,"inactivity_timeout":2.0,"connect_timeout":1.0},
#     "errors":{"1554031":{"error":"request error"}}
#     }
#
#
class JsonBatchIterator < BatchIterator
  def initialize env, queries, options={}
    @sep        = options.delete(:sep) || ""
    super
  end

  # Begin text for a hash with field "results".
  def response_preamble
    super
    @seen_first_result = false
    send "{"
    send '"results":\{', false
  end

  # Each successful response is delivered in a single line, as a single
  # chunked-transfer chunk, mapping the req_id (as a json string) to a hash with
  # the response http status code and the JSON-encoded body. NOTE: the body is
  # JSON-encoded from whatever it was! If it was already JSON, you will need to
  # call JSON.parse again on the body.
  def handle_result req_id, req
    super
    key    = JSON.generate(req_id.to_s)
    body   = JSON.generate(@results_back[req_id].slice(:status, :body))
    send( @seen_first_result ? "," : "") ; @seen_first_result = true
    send key, ":", body, false
  end

  # All unsuccessful responses are delivered together line, as a single
  # chunked-transfer chunk, in the response_coda.
  def handle_error req_id, req
    super
  end

  # After all requests complete, this dumps out a row with helpful stats, and
  # the hash of errback results.
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

# outputs results, one per line, tab-separated. Each line is
#
#     EVENT     req_id    status   body\n
#
# No changes are made to the body except to scrub it for internal CR, LF and TAB
class TsvBatchIterator < BatchIterator
  def handle_result req_id, req
    super
    send_tsv '_r', req_id, req.response_header.http_status, req.response
  end

  def handle_error req_id, req
    super
    send_tsv '_e', req_id, "", %Q{{"error":'#{req.error}'}}
  end

  def response_coda
    super
    send_tsv '_s',  '', '', JSON.generate(stats) if @show_stats
  end

  def send_tsv event, req_id, status, body
    req_id = req_id.to_s.gsub(/[\r\n\t]+/, "")
    body   = body.to_s.gsub(  /[\r\n\t]+/, "")
    @env.chunked_stream_send("#{[event, req_id, status, body].join("\t")}\n")
  end
end
