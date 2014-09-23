require 'wordtriez'
require 'wordtree'
require 'optparse'
require 'thread'
require 'celluloid'
require 'irb'

options = {
  :library => "library",
  :refs => false
}

OptionParser.new do |opts|
  opts.banner = "Usage: baseline.rb [options] BOOK_ID"

  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end

  opts.on("-l", "--library LIBRARY", "Set library path to LIBRARY") do |path|
    options[:library] = path
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


work_q = Queue.new

ref_id = 0
$lib.library.each do |path, id|
  ref_id += 1
  work_q.push([id, path, ref_id])
end

threads = (0...Celluloid.cores).map do
  Thread.new do
    puts "Creating Worker"
    Thread.current[:worker] = worker = TextWorker.new(case_study.content, options)
    begin
      while args = work_q.pop(true)
        worker.count_ngrams(*args)    
      end
    rescue ThreadError
    end
  end
end

threads.map(&:join)

$master_trie = Wordtriez.new
threads.each do |t|
  puts "Size: #{t[:worker].trie.size}"
  t[:worker].trie.each do |k, v|
    # puts k, v
    $master_trie[k] += v
  end
end

puts "Done."

IRB.start
