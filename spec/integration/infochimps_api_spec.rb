require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

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
