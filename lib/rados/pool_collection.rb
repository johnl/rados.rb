module Rados

  class PoolCollection
    def initialize(cluster)
      @cluster = cluster
    end

    def all
      @cluster.pool_list.collect do |name|
        Pool.new(:cluster => @cluster, :name => name)
      end
    end

    def size
      @cluster.pool_list.size
    end

    def each(&block)
      all.each(&block);
    end

    def find(name)
      pool = Pool.new(:cluster => @cluster, :name => name)
      if pool.id.nil?
        nil
      else
        pool
      end
    end

    def exists?(name)
      find(name).nil? ? false : true
    end

    def create(options)
      options = { :name => options } if options.is_a? String
      name = options[:name]
      if exists?(name)
        raise Rados::ErrorCreatingPool, "pool #{name} already exists"
      end
      @cluster.pool_create(name)
      find(name)
    end

    def destroy(name)
      @cluster.pool_delete(name)
    end
  end
end
