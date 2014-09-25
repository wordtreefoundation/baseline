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

  opts.on("-o", "--output FILE", "Send output to FILE") do |path|
    options[:output] = path
  end

  opts.on("", "--chdir DIR", "Change working dir to DIR before processing") do |path|
    options[:chdir] = path
  end
end.parse!

def make_word_index(lookup, path, tracker = 0, &block)
  text = File.open(path, "r:UTF-8", &:read)
  $t.text_clean(text)
  wordcount = 0
  scrubbed_text = text.scrub
  scrubbed_text.scan(/\w+/) do |word|
    wordcount += 1
    i = (lookup[word] ||= (tracker += 1))
    yield i, wordcount
  end
  [wordcount, tracker]
end

files = File.read(options[:files]).split("\n")
files = files.map{ |f| File.join(options[:chdir], f) } if options[:chdir]

lookup = {}
tracker = 0
total_words = 0
files.each_with_index do |path, i|
  puts "#{i+1}. #{Time.now.strftime("%H:%M:%S.%L")} - #{path}"
  bin_path = path.sub(/\.md$/, ".bin")
  bin_arr = []
  wordcount, tracker = make_word_index(lookup, path, tracker) do |i, w|
    bin_arr << i
  end
  puts "#{i+1}. Writing binary file #{bin_path}"
  File.open(bin_path, "wb") do |file|
    file.write bin_arr.pack('N*')
  end

  total_words += wordcount
end

puts "Total words: #{total_words}"
puts "Unique words: #{lookup.size} #{tracker}"

puts "Saving index to #{options[:output]}"

File.open(options[:output], "w:UTF-8") do |file|
  file.write lookup.to_json
end

puts "Done."