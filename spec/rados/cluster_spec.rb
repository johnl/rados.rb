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

  describe "stats" do
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
end
