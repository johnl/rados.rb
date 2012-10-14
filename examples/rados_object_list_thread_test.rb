require 'rados'

cluster = Rados::Cluster.new
puts cluster.stats.inspect

pool = ENV["POOL"] || "test"

lister = Thread.new do
  while (true)
    cluster.pool_objects_each(pool) { |o| }
    STDOUT.write("o")
  end
end

Thread.new do
  while (true)
    cluster.pool_objects_each(pool) { |o| }
    STDOUT.write("x")
  end
end

Thread.new do
  while (true)
    cluster.pool_objects_each(pool) { |o| }
    STDOUT.write("u")
  end
end

Thread.new do
  while (true)
    cluster.pool_objects_each(pool) { |o| }
    STDOUT.write("d")
  end
end

dotter = Thread.new do
  while (true)
    50000.times { }
    STDOUT.write(".")
  end
end

lister.join
