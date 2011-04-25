#!/usr/bin/env ruby
$:<< './lib'

# A simple dashboard for
#
# See
#   lib/son_of_a_batch.rb    -- batch code
#   app/endpoints            -- actual response code
#   app/views                -- templates
#   public                   -- static files
#   config/app.rb            -- configuration
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


class App < Goliath::API
  use Goliath::Rack::Params             # parse query & body params
  use Goliath::Rack::Formatters::JSON   # JSON output formatter
  use Goliath::Rack::Render             # auto-negotiate response format
  use(Rack::Static,                     # render static files from ./public
    :root => Goliath::Application.root_path("public"), :urls => ["/favicon.ico", '/stylesheets', '/javascripts', '/images'])
  use Rack::AbstractFormat, 'text/html'

  include Goliath::Rack::Templates      # render templated files from ./views

  # # plugin Goliath::Plugin::Latency       # ask eventmachine reactor to track its latency
  def recent_latency
    Goliath::Plugin::Latency.recent_latency if defined?(Goliath::Plugin::Latency)
  end

  def response(env)
    batch_id = "%7.04f" % (env[:start_time].to_f - 100*(env[:start_time].to_f.to_i / 100))
    case env['PATH_INFO']
    when '/'         then return [200, {}, haml(:root)]
    when '/debug'    then return [200, {}, haml(:debug)]
    when '/joke'     then return [200, {}, haml(:joke)]
    # when '/get'      then :pass
    else                  raise Goliath::Validation::NotFoundError
    end
  end
end

