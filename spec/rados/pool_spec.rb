# encoding: UTF-8
require 'spec_helper'

def a_pool_named(name)
  Rados::Pool.new(:cluster => @cluster, :name => name)
end

describe Rados::Pool do
  before(:all) do
    @cluster = Rados::Cluster.new
    @valid_pool_name = "data"
    @invalid_pool_name = "pooldoesntexist"
  end

  it "should take :cluster as an option" do
    cluster = double("cluster")
    pool = Rados::Pool.new(:cluster => cluster)
    pool.cluster.should == cluster
  end

  describe "#name" do
    it "should return the pool name" do
      pool = Rados::Pool.new(:name => @valid_pool_name)
      pool.name.should == @valid_pool_name
    end
  end

  describe "#id" do
    it "should call pool_lookup on the cluster" do
      @cluster.should_receive(:pool_lookup).with(@valid_pool_name).and_return(nil)
      a_pool_named(@valid_pool_name).id
    end
    it "should return the pool identifier" do
      @cluster.should_receive(:pool_lookup).with(@valid_pool_name).and_return(99)
      a_pool_named(@valid_pool_name).id.should == 99
    end
    it "should return nil for non-existant pools" do
      a_pool_named(@invalid_pool_name).id.should be_nil
    end
    it "should return nil if the cluster is not specified" do
      Rados::Pool.new(:name => "error").id.should be_nil
    end
  end

  describe "#stat" do
    it "should call pool_stat with the cluster and the pool name" do
      ioctx = double("ioctx")
      ioctx.should_receive(:pool_stat)
      Rados::IoContext.should_receive(:new).with(@cluster, @valid_pool_name).and_return(ioctx)
      a_pool_named(@valid_pool_name).stat
    end
  end

  describe "#destroy" do
    it "should call pool_delete on the cluster" do
      @cluster.should_receive(:pool_delete).with("pool").and_return(true)
      Rados::Pool.new(:cluster => @cluster, :name => "pool").destroy
    end
  end

  describe "#new_context" do
    it "should return a IoContext instance initialized with the cluster and pool name" do
      pool = Rados::Pool.new(:cluster => @cluster, :name => @valid_pool_name)
      Rados::IoContext.should_receive(:new).with(@cluster, @valid_pool_name)
      pool.new_io_context
    end
  end
end
