#
# General settings -- check this into your repo.
# For private settings, copy @config/son_of_a_batch-private.example.rb@ to
# @config/son_of_a_batch-private.rb@ and enter your keys, etc.
#

import 'son_of_a_batch-private'

config[:app_name] = 'son_of_a_batch'

config[:template] = {
  :layout_engine => :haml,
  :views         => './app/views'
}
config[:template_engines] = {
  :haml => {
    :escape_html   => true
  }
}

p config
