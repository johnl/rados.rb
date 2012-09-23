# encoding: UTF-8
require 'spec_helper'

describe Rados::PoolCollection do
  before(:all) do
    @cluster = Rados::Cluster.new
		@pools = @cluster.pools
    @pool_list = @cluster.pool_list
  end

  describe "#all" do
    it "should return an instance of Pool for each pool" do
      @pool_list.size.should == @pools.size
      @pools.all.each do |pool|
        pool.should be_a Rados::Pool
        @pool_list.should include pool.name
      end
    end
  end

  describe "#create" do
    before(:all) do
      @new_pool_name = "ruby_rados_test_#{rand(0xfffff)}"
    end
    after(:all) do
      @cluster.pool_delete(@new_pool_name)
    end

    it "should return a Pool instance for the newly created pool" do
      pool = @pools.create(@new_pool_name)
      pool.should be_a Rados::Pool
      pool.name.should == @new_pool_name
      pool.id.should_not be_nil
      pool.cluster.should == @cluster
    end

    it "should raise an error if the pool already exists" do
      lambda { @pools.create(@new_pool_name) }.should raise_error(Rados::ErrorCreatingPool)
    end
  end

  describe "#destroy" do
    before(:all) do
      @new_pool_name = "ruby_rados_test_#{rand(0xfffff)}"
      @cluster.pool_create(@new_pool_name)
    end

    it "should return true if the pool was destroyed" do
      @pools.destroy(@new_pool_name).should == true
      @pools.find(@new_pool_name).should == nil
    end

    it "should raise an error if the pool doesn't exist" do
      lambda { 
        @pools.destroy("this_pool_should_not_exist")
      }.should raise_error(Rados::PoolNotFound)
    end

  end
end
