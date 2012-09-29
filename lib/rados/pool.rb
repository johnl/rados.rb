module Rados
  class PoolError < Error ; end
  class ErrorCreatingPool < PoolError ; end
  class PoolNotFound < PoolError ; end

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

    def destroy
      @cluster.pool_delete(name)
    end

    def stat
      @cluster.pool_stat(name)
    end
  end
end
