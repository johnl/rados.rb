module Rados
  class PoolNotFound < Error ; end

  class Pool
    attr_reader :name, :cluster

    def initialize(options = {})
      @name = options[:name]
      @cluster = options[:cluster]
    end

    def id
      raise Rados::PoolNotFound if @cluster.nil?
      @cluster.pool_lookup(@name)
    rescue Rados::PoolNotFound
      nil
    end
  end
end
