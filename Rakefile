require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'rake'

require 'jeweler'
Jeweler::Tasks.new do |gem|
  # gem is a Gem::Specification... see http://docs.rubygems.org/read/chapter/20 for more options
  gem.name = "son_of_a_batch"
  gem.homepage = "http://infochimps.com/labs"
  gem.license = "MIT"
  gem.summary = %Q{Smelt from a plentiferous gallimaufry of requests an agglomerated bale of responses. With, y'know, concurrency and all that.}
  gem.description = %Q{Smelt from a plentiferous gallimaufry of requests an agglomerated bale of responses. With, y'know, concurrency and all that.}
  gem.email = "coders@infochimps.com"
  gem.authors = ["Philip (flip) Kromer for Infochimps"]

  gem.required_ruby_version = '>=1.9.2'

  gem.add_dependency 'goliath',             ">= 0.9.1"
  gem.add_dependency 'eventmachine',        ">= 1.0.0.beta.3"
  gem.add_dependency 'em-synchrony',        ">= 0.3.0.beta.1"
  gem.add_dependency 'em-http-request',     ">= 1.0.0.beta.3"

  gem.add_dependency 'yajl-ruby',           "~> 0.8.2"
  gem.add_dependency 'gorillib',            "~> 0.1.1"
  gem.add_dependency 'addressable',         "~> 2.2.5"

  gem.add_dependency 'rack',                ">=1.2.2"
  gem.add_dependency 'rack-contrib'
  gem.add_dependency 'rack-respond_to',     "~> 0.9.8"
  gem.add_dependency 'rack-abstract-format',"~> 0.9.9"
  gem.add_dependency 'async-rack'
  gem.add_dependency 'multi_json'

  gem.add_development_dependency 'bundler', "~> 1.0.12"
  gem.add_development_dependency 'yard',    "~> 0.6.7"
  gem.add_development_dependency 'jeweler', "~> 1.5.2"
  gem.add_development_dependency 'rspec',   "~> 2.5.0"
  gem.add_development_dependency 'rcov',    ">= 0.9.9"
  gem.add_development_dependency 'spork',   "~> 0.9.0.rc5"
  gem.add_development_dependency 'watchr'

  ignores = File.readlines(".gitignore").grep(/^[^#]\S+/).map{|s| s.chomp }
  dotfiles = [".gemtest", ".gitignore", ".rspec", ".yardopts", ".bundle", ".vendor"]
  gem.files = dotfiles + Dir["**/*"].
    reject{|f| f =~ /^\.vendor\// }.
    reject{|f| File.directory?(f) }.
    reject{|f| ignores.any?{|i| File.fnmatch(i, f) || File.fnmatch(i+'/**/*', f) } }
  gem.test_files = gem.files.grep(/^spec\//)
  gem.require_paths = ['lib']
end
Jeweler::RubygemsDotOrgTasks.new

require 'rspec/core'
require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
end

RSpec::Core::RakeTask.new(:rcov) do |spec|
  spec.pattern = 'spec/**/*_spec.rb'
  spec.rcov = true
end

task :default => :spec

require 'yard'
YARD::Rake::YardocTask.new
