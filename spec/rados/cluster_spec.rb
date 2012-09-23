# encoding: UTF-8
require 'spec_helper'

describe Rados::Cluster do
  before(:all) do
    @cluster = Rados::Cluster.new
  end

  describe "#pool_list" do
    it "should return an array of pool names" do
      pl = @cluster.instance_eval("pool_list")
      pl.should be_a Array
      pl.size.should > 2
      pl.should include("data")
    end
  end

  describe "pools" do
    it "should return an array of instances of Pool" do
      pl = @cluster.pools
      pl.size.should == @cluster.instance_eval("pool_list").size
      pl.each { |p| p.should be_a Rados::Pool }
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
