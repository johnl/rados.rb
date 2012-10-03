# encoding: UTF-8
require 'spec_helper'

describe Rados::IoContext do
  before(:all) do
    @cluster = Rados::Cluster.new
  end

  it "should support initialization using a cluster" do
    Rados::IoContext.new(@cluster, "data")
  end

  describe "#get_id" do
    it "should return the id of the pool" do
      Rados::IoContext.new(@cluster, "data").get_id.should == @cluster.pool_lookup("data")
    end
  end

  describe "#pool_stat" do
    before :all do
      @ioctx = Rados::IoContext.new(@cluster, "data")
      @stats = @ioctx.pool_stat
    end
    it "should return a hash of stats" do
      @stats.should be_a Hash
    end
    it "should be Symbols and Fixnums" do
      @stats.each do |k,v|
        k.should be_a Symbol
        v.should be_a Fixnum
      end
    end
    it "should return expected stats" do
      [:num_objects, :num_bytes, :num_kb, :num_objects, :num_object_clones, :num_object_copies,
       :num_objects_missing_on_primary, :num_objects_unfound, :num_objects_degraded, :num_rd,
       :num_rd_kb, :num_wr, :num_wr_kb].each do |k|
        @stats.keys.should include k
      end
    end

  end
end
