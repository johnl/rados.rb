module Rados
  class Pool
    attr_reader :name, :cluster
    def initialize(options = {})
      @name = options[:name]
      @cluster = options[:cluster]
    end
  end
end
