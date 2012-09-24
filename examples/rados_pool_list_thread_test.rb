require 'rados'

cluster = Rados::Cluster.new
puts cluster.stats.inspect

statter = Thread.new do
  while (true)
    cluster.pools.each do |pool|
      cluster.pool_stat(pool.name)
      STDOUT.write("o")
    end
  end
end

dotter = Thread.new do
  while (true)
    10000.times { }
    STDOUT.write(".")
  end
end

statter.join
