module Rados

  class PoolCollection
    include Enumerable

    def initialize(cluster)
      @cluster = cluster
    end

    def all
      entries
    end

    def count
      @cluster.pool_list.size
    end

    def each(&block)
      @cluster.pool_list.collect do |name|
        block.call(Pool.new(:cluster => @cluster, :name => name))
      end
    end

    def find_by_name(name)
      pool = Pool.new(:cluster => @cluster, :name => name)
      if pool.id.nil?
        nil
      else
        pool
      end
    end

    def exists?(name)
      find_by_name(name).nil? ? false : true
    end

    def create(options)
      options = { :name => options } if options.is_a? String
      name = options[:name]
      if exists?(name)
        raise Rados::ErrorCreatingPool, "pool #{name} already exists"
      end
      @cluster.pool_create(name)
      find_by_name(name)
    end

    def destroy(name)
      @cluster.pool_delete(name)
    end
  end
end
