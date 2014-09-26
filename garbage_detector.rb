require 'optparse'
require 'fileutils'
require 'wordtriez'
require 'wordtree'
require 'benchmark'
require 'json'

options = {:output => "index.json"}
$t = Wordtriez.new

OptionParser.new do |opts|
  opts.banner = "Usage: compare.rb [options]"

  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end

  opts.on("-f", "--files FILELIST", "Read files from FILELIST.") do |path|
    options[:files] = path
  end

  opts.on("", "--chdir DIR", "Change working dir to DIR before processing") do |path|
    options[:chdir] = path
  end
end.parse!

files = File.read(options[:files]).split("\n")
files = files.map{ |f| File.join(options[:chdir], f) } if options[:chdir]

files.each_with_index do |path, i|
  time = Time.now.strftime("%H:%M:%S.%L")

  text = File.open(path, "r:UTF-8", &:read).scrub
  WordTree::Text.clean(text)
  common_count = WordTree::Text.common_trigrams(text)
  total_count = text.size - 2

  puts "#{i+1}\t#{time}\t#{common_count}\t#{total_count}\t#{common_count.to_f / total_count}\t#{path}"
  $stdout.flush
end