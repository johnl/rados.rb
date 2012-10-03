require 'rados'

cluster = Rados::Cluster.new
puts cluster.stats.inspect

statter = Thread.new do
  ioctxes = cluster.pools.collect { |p| p.new_io_context }
  while (true)
    ioctxes.each do |ioctx|
      ioctx.pool_stat
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
