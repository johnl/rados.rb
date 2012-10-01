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
end