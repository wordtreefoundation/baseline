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

def make_word_index(lookup, path, tracker = 0)
  text = File.open(path, "r:UTF-8", &:read)
  $t.text_clean(text)
  wordcount = 0
  text.scrub.scan(/\w+/) do |word|
    wordcount += 1
    lookup[word] ||= (tracker += 1)
  end
  [wordcount, tracker]
end

files = File.read(options[:files]).split("\n")
files = files.map{ |f| File.join(options[:chdir], f) } if options[:chdir]

lookup = {}
tracker = 0
total_words = 0
files.each do |path|
  puts path
  wordcount, tracker = make_word_index(lookup, path, tracker)
  total_words += wordcount
end

puts "Total words: #{total_words}"
puts "Unique words: #{lookup.size} #{tracker}"

puts "Saving index to #{options[:output]}"

File.open(options[:output], "w:UTF-8") do |file|
  file.write lookup.to_json
end

puts "Done."