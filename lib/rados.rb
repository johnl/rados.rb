module Rados
  class Error < StandardError ; end
end

require 'rados/rados'
require 'rados/pool'
require 'rados/pool_collection'
require 'rados/cluster'
require 'rados/object'
