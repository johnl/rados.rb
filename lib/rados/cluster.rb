module Rados
  class ConnectionTimeout < Error ; end
  class IOError < Error ; end
  class WriteError < IOError ; end
  class ReadError < IOError ; end

  # Cluster represents a connection to a Ceph cluster
  class Cluster
    def initialize(options = {})
      initialize_ext
      
      options
    end

    def pools
      @pool_collection ||= PoolCollection.new(self)
    end

  end
end
