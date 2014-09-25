require 'optparse'
require 'fileutils'
require 'wordtriez'
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

common_trigrams = %w(all and edt ent ere for has hat her his ing ion ith men nce nde oft sth ter tha the thi tio tis ver was wit you)

files.each_with_index do |path, i|
  time = Time.now.strftime("%H:%M:%S.%L")
  # puts "#{i+1}. #{time} - #{path}"

  text = File.open(path, "r:UTF-8", &:read)
  common_count = 0
  ss = StringScanner.new(text)
  while !ss.eos?
    trigram = ss.peek(3)
    common_count += 1 if common_trigrams.include?(trigram)
    ss.pos += 1
  end
  total_count = text.size - 2

  puts "#{i+1}\t#{time}\t#{common_count}\t#{total_count}\t#{common_count.to_f / total_count}\t#{path}"
end