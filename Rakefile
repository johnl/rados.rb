require "rake/extensiontask"

Rake::ExtensionTask.new("rados") do |ext|
  ext.lib_dir = File.join 'lib', 'rados'
end

begin
  require 'rspec'
  require 'rspec/core/rake_task'

  RSpec::Core::RakeTask.new('spec') do |t|
    t.verbose = true
  end

  task :default => :spec
rescue LoadError
  puts "rspec, or one of its dependencies, is not available. Install it with: sudo gem install rspec"
end

Rake::Task[:spec].prerequisites << :compile
