# encoding: UTF-8
require 'spec_helper'

describe Rados::RObject do
  before(:each) do
    @ioctx = mock(:iocontext)
    @o = Rados::RObject.new("test", @ioctx)
  end

  it "should take an oid and an ioctx on instantiation" do
    o = Rados::RObject.new("test", @ioctx)
    o.ioctx.should == @ioctx
    o.oid.should == "test"
  end

  describe "#tell" do
    it "should return the value of the offset" do
      @o.tell.should == 0
      @ioctx.should_receive(:read).and_return("somedata")
      @o.read
      @o.tell.should == 8
    end
  end

  describe "#read" do
    it "should increase the offset by the size of the read" do
      @ioctx.should_receive(:read).and_return(8)
      @o.tell.should == 0
      @o.read
      @o.tell.should == 8
    end

    it "should read from the current offset" do
      @ioctx.should_receive(:read).and_return("somedata")
      @o.read
      @ioctx.should_receive(:read).with("test", nil, 8).and_return("moredata")
      @o.read
    end
  end

  describe "#write" do
    it "should increase the offset by the size of the write" do
      @ioctx.should_receive(:write).and_return(8)
      @o.tell.should == 0
      @o.write("somedata")
      @o.tell.should == 8
    end

    it "should write to the current offset" do
      @ioctx.should_receive(:write).with("test", "somedata", 8, 0).and_return(8)
      @o.write("somedata")
      @ioctx.should_receive(:write).with("test", "moredata", 8, 8).and_return(8)
      @o.write("moredata")
    end
  end

  describe "#rewind" do
    it "should set the internal offset back to 0" do
      @ioctx.should_receive(:read).with("test", nil, 0).and_return("somedata")
      @o.read
      @o.rewind
      @ioctx.should_receive(:read).with("test", nil, 0).and_return("moredata")
      @o.read
    end
  end

  describe "#seek" do
    it "should set offset to amount plus current position when IO::SEEK_CUR is given" do
      @o.tell.should == 0
      @o.seek(888, IO::SEEK_CUR)
      @o.tell.should == 888
      @o.seek(-111, IO::SEEK_CUR)
      @o.tell.should == 777
      @o.seek(222, IO::SEEK_CUR)
      @o.tell.should == 999
    end
    it "should set offset to absolute location given by amount when IO::SEEK_SET is given" do
      @o.tell.should == 0
      @o.seek(555, IO::SEEK_SET)
      @o.tell.should == 555
      @o.seek(111, IO::SEEK_SET)
      @o.tell.should == 111
      lambda {
        @o.seek(-10, IO::SEEK_SET)
      }.should raise_exception Errno::EINVAL
    end
  end

end
