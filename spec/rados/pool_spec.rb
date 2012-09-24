# encoding: UTF-8
require 'spec_helper'

describe Rados::Pool do
  before(:all) do
    @cluster = Rados::Cluster.new
  end

  it "should take :cluster as an option" do
    Rados::Pool.new(:cluster => @cluster).cluster.should == @cluster
  end

  describe "#name" do
    it "should return the pool name" do
      Rados::Pool.new(:name => "mypool").name.should == "mypool"
    end
  end

  describe "#id" do
    it "should return the pool identifier" do
      p = Rados::Pool.new(:cluster => @cluster, :name => @cluster.pool_list.first)
      p.id.should >= 0
    end
    it "should return nil for non-existant pools" do
      p = Rados::Pool.new(:cluster => @cluster, :name => "this_pool_does_not_exist")
      p.id.should be_nil
    end
    it "should return nil if the cluster is not specified" do
      Rados::Pool.new(:name => "error").id.should be_nil
    end
  end

end
