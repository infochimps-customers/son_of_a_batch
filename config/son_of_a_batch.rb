#
# General settings -- check this into your repo.
# For private settings, copy @config/son_of_a_batch-private.example.rb@ to
# @config/son_of_a_batch-private.rb@ and enter your keys, etc.
#

p ['loading config', __FILE__]

import 'son_of_a_batch-private'

config[:template] = {
  :layout_engine => :haml,
  :views         => './app/views'
}
config[:template_engines] = {
  :haml => {
    :escape_html   => true
  }
}
