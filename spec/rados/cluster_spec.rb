# encoding: UTF-8
require 'spec_helper'

describe Rados::Cluster do
  before(:all) do
    @cluster = Rados::Cluster.new
  end

  describe "#pools" do
    it "should return a PoolCollection" do
      pools = @cluster.pools
      pools.should be_a Rados::PoolCollection
      pools.instance_eval("@cluster").should == @cluster
    end
  end

  describe "#pool_list" do
    it "should return an array of pool names" do
      pl = @cluster.pool_list
      pl.should be_a Array
      pl.size.should > 2
      pl.should include("data")
    end
  end

  describe "#pool_lookup" do
    before(:all) do
      @pool_list = @cluster.pool_list
    end
    it "should return the id of an existing pool" do
      @cluster.pool_lookup(@pool_list.first).should >= 0
    end
    it "should raise an exception if a pool doesn't exist" do
      lambda {
        @cluster.pool_lookup("this_pool_doesnt_exist")
      }.should raise_error(Rados::Error)
    end
    it "should raise a type error when given a non-string object" do
      lambda {
        @cluster.pool_lookup(:some_symbol)
      }.should raise_error(TypeError)
    end
  end

  describe "#pool_create" do
    it "should raise a type error when given a non-string object" do
      lambda {
        @cluster.pool_create(:some_symbol)
      }.should raise_error(TypeError)
    end
    it "should return true when it can create a pool" do
      @cluster.pool_create("ruby_rados_test").should == true
    end
  end

  describe "#pool_delete" do
    before(:all) do
      @new_pool_name = "ruby_rados_test_#{rand(0xfffff)}"
    end

    it "should raise a type error when given a non-string object" do
      lambda {
        @cluster.pool_delete(:some_symbol)
      }.should raise_error(TypeError)
    end
    it "should return true when the pool is deleted" do
      @cluster.pool_create(@new_pool_name)
      @cluster.pool_delete(@new_pool_name).should == true
    end
    it "should raise an error if the pool doesn't exist" do
      lambda {
        @cluster.pool_delete(@new_pool_name)
      }.should raise_error(Rados::PoolNotFound)
    end
  end

  describe "#stats" do
    before(:all) do
      @stats = @cluster.stats
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
      [:num_objects, :kb, :kb_used, :kb_avail].each do |k|
        @stats.keys.should include k
      end
    end
  end

  describe "#pool_stat with a pool_name" do
    before(:all) do
      @valid_pool_name = @cluster.pool_list.first
      @stats = @cluster.pool_stat(@valid_pool_name, nil)
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

  describe "#pool_stat with an IoContext" do
    before(:all) do
      @valid_pool_name = @cluster.pool_list.first
    end
    it "should return a hash of stats using an IoContext" do
      @ioctx = Rados::IoContext.new(@cluster, @valid_pool_name)
      @stats = @cluster.pool_stat(nil, @ioctx)
      @stats.should be_a Hash
      @stats.keys.should include :num_objects
    end
  end

  describe "#pool_objects_each" do
    it "should" do
      object_names = []
      @cluster.pool_objects_each("test") { |o| object_names << o }
      object_names.each { |o| o.should be_a String }
      object_names.size.should > 2
    end
  end
end
