module Rados
  # Cluster represents a connection to a Ceph cluster
  class Cluster
    # Requires :config_file with a path to the ceph config file
    def initialize(options = {})
      @config_file = options[:config_file]
      initialize_ext
      options
    end

    def pools
      pool_list.collect do |name|
        Pool.new(:name => name, :cluster => self)
      end
    end
  end
end
