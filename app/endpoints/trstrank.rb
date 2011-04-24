#!/usr/bin/env ruby
$:<< './lib'

require 'boot'
require 'goliath'
require 'em-synchrony/em-http'
require 'gorillib'
require 'yajl/json_gem'

require 'son_of_a_batch'

class Trstrank < Goliath::API
  use Goliath::Rack::Params             # parse query & body params
  use Goliath::Rack::ValidationError    # catch and render validation errors
  use Goliath::Rack::Validation::NumericRange, {:key => '_timeout', :min => 1.5, :max => 10.0, :default => 5.0, :as => Float}
  include LogJammin

  JsonBatchIterator.send(:include, LoggingIterator)
  TsvBatchIterator.send(:include, LoggingIterator)

  SLEEPY_URL_BASE = "http://localhost:9002/meta/http/sleepy.json"
  SLEEPY_QUERIES = [ 1.0, 14.5, 5.0, 2.5, 4.0, 3.3, 3.0, 1.6 ].each_with_index.map{|delay, idx| [idx, "#{SLEEPY_URL_BASE}/?delay=#{delay}"] }
  def sleepy_queries
    SLEEPY_QUERIES
  end
  
  TRSTRANK_URL_BASE = "http://api.infochimps.com/social/network/tw/influence/trstrank.json"
  SCREEN_NAMES    = %w[ mrflip infochimps justinbieber thedatachef damon cheaptweet austinonrails aseever  mrflip infochimps justinbieber thedatachef damon cheaptweet austinonrails aseever ]
  def trstrank_queries
    {}.tap{|h| SCREEN_NAMES.each{|sn| h[sn] = "#{TRSTRANK_URL_BASE}?_apikey=#{config[:infochimps_apikey]}&screen_name=#{sn}" } }
  end

  def response(env)
    batch_id   = "%7.04f" % (env[:start_time].to_f - 100*(env[:start_time].to_f.to_i / 100))
    sep        = (params['_pretty'].to_s == 'true') ? "\n" : ""
    show_stats = (params['_show_stats'].to_s  == 'true')

    logline env, batch_id, 0, "group", "building"
    TsvBatchIterator.new(env, sleepy_queries,
      :timeout      => params['_timeout'],
      :batch_id     => batch_id,
      :sep          => sep,
      :show_stats   => show_stats
      ).perform

    logline env, batch_id, 0, "group", "built"
    chunked_streaming_response(200, {'X-Responder' => self.class.to_s, })
  end
end
