require 'rados'

cluster = Rados::Cluster.new
puts cluster.stats.inspect

pool = ENV["POOL"] || "test"

writer = Thread.new do
  ioctx = Rados::IoContext.new(cluster, pool)
  o = ioctx.open("test")
  while true
    o.write("helloworld")
    STDOUT.write("w")
  end
end

reader = Thread.new do
  ioctx = Rados::IoContext.new(cluster, pool)
  o = ioctx.open("test")
  while true
    o.read(8)
    STDOUT.write("r")
  end
end

dotter = Thread.new do
  while (true)
    50000.times { }
    STDOUT.write(".")
  end
end

dotter.join
