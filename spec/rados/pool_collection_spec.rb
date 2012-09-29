# encoding: UTF-8
require 'spec_helper'

describe Rados::PoolCollection do
  before(:each) do
    @cluster = double("cluster")
    @pool_list = %w{pool1 pool2 pool3 pool4 pool5 pool6}
    @new_pool_name = "newpool"
    @existing_pool_name = "pool1"
    @cluster.stub(:pool_list).and_return(@pool_list)
    @cluster.stub(:pool_lookup) do |arg|
      raise Rados::PoolNotFound if arg == @new_pool_name
      99
    end
    @pool_collection = Rados::PoolCollection.new(@cluster)
  end

  describe "#count" do
    it "should return the number of pools that exist" do
      @pool_collection.count.should == @pool_list.size
    end
  end

  describe "#all" do
    it "should return an instance of Pool for each pool" do
      @pool_collection.all.size.should == @pool_list.size
      @pool_collection.all.each do |pool|
        pool.should be_a Rados::Pool
        @pool_list.should include pool.name
      end
    end
  end

  describe "#each" do
    it "should execute the given block for each pool that exists" do
      pl = []
      @pool_collection.each { |p| pl << p.name }
      pl.should == @pool_list
    end
  end

  describe "#create" do
    it "should return a Pool instance for the newly created pool" do
      @cluster.should_receive(:pool_create).with(@new_pool_name).and_return(true)
      pool = @pool_collection.create(:name => @new_pool_name)
      pool.should be_a Rados::Pool
      pool.name.should == @new_pool_name
      pool.cluster.should == @cluster
    end

    it "should raise an error if the pool already exists" do
      lambda { @pool_collection.create(:name => @existing_pool_name) }.should raise_error(Rados::ErrorCreatingPool)
    end
  end

  describe "#destroy" do
    it "should return true if the pool was destroyed" do
      @cluster.should_receive(:pool_delete).with(@existing_pool_name).and_return(true)
      @pool_collection.destroy(@existing_pool_name).should == true
    end

    it "should raise an error if the pool doesn't exist" do
      @cluster.should_receive(:pool_delete).with(@new_pool_name).and_raise(Rados::PoolNotFound)
      lambda { 
        @pool_collection.destroy(@new_pool_name)
      }.should raise_error(Rados::PoolNotFound)
    end

  end
end
