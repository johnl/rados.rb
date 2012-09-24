require 'rados'

cluster = Rados::Cluster.new
puts cluster.stats.inspect
cluster.pools.each do |pool|
  puts pool.name
  puts cluster.pool_stat(pool.name).inspect
  puts
end
