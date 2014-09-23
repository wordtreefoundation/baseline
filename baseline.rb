require 'wordtriez'
require 'wordtree'
require 'optparse'
require 'thread'
# require 'irb'
require 'json'

options = {
  :library => "library",
  :refs => false,
  :output => "baseline.json"
}

OptionParser.new do |opts|
  opts.banner = "Usage: baseline.rb [options] BOOK_ID"

  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end

  opts.on("-l", "--library LIBRARY", "Set library path to LIBRARY") do |path|
    options[:library] = path
  end

  opts.on("-o", "--output BASELINE", "Set baseline file path to BASELINE") do |path|
    options[:output] = path
  end

  opts.on(nil, "--[no-]refs", "Include references to books (uses a lot of memory)") do |bool|
    options[:refs] = bool
  end

end.parse!


if ARGV.size < 1
  puts "book ID expected"
  exit -1
else
  case_id = ARGV.shift
end

if !File.directory?(options[:library])
  puts "library not found: #{options[:library]}"
  exit -1
end

$lib = WordTree::Disk::Librarian.new(options[:library])
$start = Time.now

class TextWorker
  attr_reader :trie

  def initialize(source_text, options, n=4)
    @trie = Wordtriez.new
    @trie.add_text!(source_text.downcase, n)
    @options = options
  end

  def load_text(path)
    IO.read(path)
  end

  def count_ngrams(book_id, path, ref_id)
    puts "#{ref_id}: Processing #{book_id}..."
    text = load_text(path)
    puts "#{ref_id}: loaded from disk (#{text.size}b)"
    begin
      @trie.union_text!(text, 4, @options[:refs] ? '-' + id : '')
    rescue StandardError => e
      puts "#{ref_id}: **ERROR** while adding text: #{text[0..100]}... #{e}"
    end
    Time.now.tap do |now|
      puts "#{ref_id}: added (#{now - $start} seconds since start - #{(now - $start).to_f / ref_id} avg), new size: #{@trie.size}"
    end
  end
end

case_study = $lib.find_without_ngrams(case_id)
# $pool = TextWorker.pool(args: [case_study.content, options])

worker = TextWorker.new(case_study.content, options)

content_q = Queue.new

ref_id = 0

start = Time.now

io_thread = Thread.new do
  $lib.library.each do |path, id|
    ref_id += 1
    content_q.push([id, path, ref_id])
  end
end

work_thread = Thread.new do
  sleep 0.01 while ref_id == 0
  begin
    while args = content_q.pop(true)
      worker.count_ngrams(*args)    
    end
  rescue ThreadError
  end
end

io_thread.join
work_thread.join

finish = Time.now

puts "Done."

meta =   {
  "_meta" => {
    "description" => "Baseline word frequencies for unigrams to 4-grams of words in the book #{case_id}",
    "book_count" => ref_id,
    "book_range_start_year" => 1750,
    "book_range_end_year" => 1860,
    "processing_time_in_seconds" => finish - start,
    "processing_time_avg_per_book" => (finish - start).to_f / ref_id
  }
}

File.open(options[:output], "w") do |file|
  file.puts "{"
  file.write JSON.pretty_generate(meta).split("\n").to_a[1..-2].join("\n") + ",\n"
  worker.trie.each do |k, v|
    file.puts "  \"#{k}\": #{v},"
  end
  file.puts "}"
end

# $t = worker.trie
# IRB.start
