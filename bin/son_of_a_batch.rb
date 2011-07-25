#!/usr/bin/env ruby

module Goliath ; ROOT_DIR = File.expand_path(File.dirname(__FILE__)+'/..') ; end
require File.join(File.dirname(__FILE__), '../lib/use_gemfile_jail')
$LOAD_PATH.unshift(Goliath.root_path("lib")) unless $LOAD_PATH.include?(Goliath.root_path("lib"))

require 'postrank-uri'
require 'set'
require 'gorillib/numeric/clamp'
require 'gorillib/hash/slice'

require 'senor_armando'
require 'son_of_a_batch/batch_iterator'

require 'rack/mime'
Rack::Mime::MIME_TYPES['.tsv'] = "text/tsv"

#
# Using the simple get batch (many params against one host)
#   curl -v 'http://localhost:9004/batch.json?url_base=http%3A%2F%2Flocalhost%3A9002%2F%3Fdelay%3D&url_vals=1.0,2.3,1.3,4.0&_pretty=true&_show_stats=true&_timeout=1.9'
#
# The JSON form of that:
#   curl -v -H "Content-Type: application/json" --data-ascii '{ "_pretty":true, "_show_stats":true, "_timeout":1.9, "url_base":"http://localhost:9002/?delay=", "url_vals":"1.0,2.3,1.3,4.0" }' 'http://localhost:9004/batch.json'
#
# Arbitrary assemblage of URLs (all hosts must be whitelisted)
#
#   APIKEY=XXXXX
#   curl -v -H "Content-Type: application/json" --data-ascii '{"_pretty":true,"_show_stats":true,"_timeout":1.9,"urls":{
#     "food":"http://api.infochimps.com/social/network/tw/search/people_search?_apikey='$APIKEY'&q=food",
#     "drink":"http://api.infochimps.com/social/network/tw/search/people_search?_apikey='$APIKEY'&q=drink",
#     "sex":"http://api.infochimps.com/social/network/tw/search/people_search?_apikey='$APIKEY'&q=sex",
#     "bieber":"http://api.infochimps.com/social/network/tw/search/people_search?_apikey='$APIKEY'&q=bieber",
#     "mrflip":"http://api.infochimps.com/social/network/tw/influence/trstrank.json?_apikey='$APIKEY'&screen_name=mrflip"
#   }' 'http://localhost:9004/batch.json'
#
# Commandline is an IDE FTW:
#
#   APIKEY=XXXXX
#   curl 'http://api.infochimps.com/social/network/tw/graph/strong_links?_apikey='$APIKEY'&screen_name=infochimps' > /tmp/strong_links_raw.txt
#   cat /tmp/strong_links_raw.txt | ruby -rjson -e 'res = JSON.parse($stdin.read); puts res["strong_links"].map{|id,sc| id }.join(",") ' > /tmp/strong_links_ids.txt
#   curl -H "Content-Type: application/json" --data-ascii '{ "_pretty":true, "_show_stats":true, "_timeout":1.9, "url_base":"http://api.infochimps.com/social/network/tw/influence/trstrank.json?_apikey='$APIKEY'&user_id=", "url_vals":"'`cat /tmp/strong_links_ids.txt`'" }' -v 'http://localhost:9004/batch.json'
#
class SonOfABatch < Goliath::API
  use Goliath::Rack::Heartbeat             # respond to /status with 200, OK (monitoring, etc)
  use Goliath::Rack::Tracer                # log trace statistics
  use Goliath::Rack::Params                # parse & merge query and body parameters
  use SenorArmando::Rack::ExceptionHandler # catch errors and present as non-200 responses

  # JsonBatchIterator.class_eval{ include SonOfABatch::LoggingIterator }
  # TsvBatchIterator.class_eval{  include SonOfABatch::LoggingIterator }

  HOST_WHITELIST = %w[
    api.infochimps.com localhost 127.0.0.1
  ].to_set

  def response(env)
    options = get_options
    requestor = case
                when env['PATH_INFO'] =~ %r{/batch\.json$} then JsonBatchIterator
                when env['PATH_INFO'] =~ %r{/batch\.tsv$}  then TsvBatchIterator
                else raise NotAcceptableError, "Only .json and .tsv are supported for son_of_a_batch"
                end

    # launch the requests; response will stream back asynchronously
    requestor.new(env, options.delete(:queries), options).perform

    headers = {'X-Responder' => self.class.to_s, 'X-Sob-Timeout' => options[:timeout].to_s }
    chunked_streaming_response(200, headers)
  end

protected

  # make the given URL string safe.
  # TODO: be even more of a dick.
  def normalize_query url
    url = PostRank::URI.normalize(url) rescue nil
    return if url.blank?
    return if url.host.blank? || (! HOST_WHITELIST.include?(url.host))
    return unless (url.scheme == 'http')
    url
  end

  # Turn the raw params hash into actionable values.
  def get_options
    batch_id      = "%7.04f" % (env[:start_time].to_f - 100*(env[:start_time].to_f.to_i / 100))
    show_stats    = params['_show_stats'].to_s == 'true'
    sep           = params['_pretty'    ].to_s == 'true' ? "\n" : ""
    timeout       = params['_timeout'].present? ? params['_timeout'].to_f.clamp(2.0, 10.0) : nil

    if params['urls'] && params['urls'].is_a?(Hash)
      # the outcome of a json-encoded POST body
      queries = params['urls']
    elsif params['url_base'] && params['url_vals']
      # assemble queries by slapping each url_val on the end of url_base
      url_vals = params['url_vals']
      url_vals = url_vals.to_s.split(',') unless url_vals.is_a?(Array)
      queries = {}.tap{|q| url_vals.each_with_index{|val,idx| q[idx.to_s] = "#{params['url_base']}#{val}" } }
    else
      raise BadRequestError, "Need either url_base and url_vals, or a JSON post body giving a hash of req_id:url pairs."
    end

    # make all the queries safe
    queries.each{|req_id, q| queries[req_id] = normalize_query(q) }
    queries.compact!

    { :batch_id => batch_id, :sep => sep, :show_stats => show_stats, :timeout => timeout, :queries => queries, }.compact
  end
end
