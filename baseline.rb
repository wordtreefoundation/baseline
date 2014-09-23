require 'wordtriez'
require 'wordtree'
require 'optparse'
require 'thread'
require 'irb'
require 'json'

class TextWorker
  attr_reader :trie

  def initialize(source_text=nil, add_refs=false, n=4)
    @trie = Wordtriez.new
    @trie.add_text!(source_text.downcase, n) if source_text
    # Restrict keys of subsequent books if we have a source_text
    @restrict_keys = !source_text.nil?
    @add_refs = add_refs
  end

  def load_text(path)
    IO.read(path)
  end

  def count_ngrams(book_id, path, ref_id)
    puts "#{ref_id}: Processing #{book_id}..."
    text = load_text(path)
    puts "#{ref_id}: loaded from disk (#{text.size}b)"
    begin
      if @restrict_keys
        @trie.union_text!(text, 4, @add_refs ? '-' + id : '')
      else
        @trie.add_text!(text, 4, @add_refs ? '-' + id : '')
      end
    rescue StandardError => e
      puts "#{ref_id}: **ERROR** while adding text: #{text[0..100]}... #{e}"
    end
    Time.now.tap do |now|
      puts "#{ref_id}: added (#{now - $start} seconds since start - #{(now - $start).to_f / ref_id} avg), new size: #{@trie.size}"
    end
  end
end


options = {
  :library => "library",
  :refs => false,
  :format => "txt",
  :n => 4
}

OptionParser.new do |opts|
  opts.banner = "Usage: baseline.rb [options]"

  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end

  opts.on("-l", "--library LIBRARY", "Set library path to LIBRARY. Use '-' to take file paths from STDIN.") do |path|
    options[:library] = path
  end

  opts.on("-o", "--output BASELINE", "Set baseline file path to BASELINE") do |path|
    options[:output] = path
  end

  opts.on("-r", "--restrict FILE", "Restrict baseline to ngrams found in text file FILE") do |path|
    options[:restrict] = path
  end

  opts.on("-f", "--format FORMAT", "Output format: 'txt' or 'json'") do |format|
    options[:format] = format
  end

  opts.on("-n", "--ngrams N", "Generate x-grams from 1 to N") do |n|
    options[:n] = Integer(n)
  end

  opts.on(nil, "--restrict-id BOOK_ID", "Restrict baseline to ngrams found in BOOK_ID from library") do |book_id|
    options[:restrict_book_id] = book_id
  end

  opts.on(nil, "--[no-]refs", "Include references to books (uses a lot of memory)") do |bool|
    options[:refs] = bool
  end
end.parse!

options[:output] ||= "baseline.#{options[:format]}"

if options[:library] == "-"
  # use stdin
else
  if !File.directory?(options[:library]) 
    puts "library not found: #{options[:library]}"
    exit -1
  end
  $lib = WordTree::Disk::Librarian.new(options[:library])
end

$start = Time.now

if options[:restrict]
  book_id = WordTree::Disk::LibraryLocator.id_from_path(options[:restrict])
  begin
    retrieved = Preamble.load(options[:restrict], :external_encoding => "utf-8")
    case_study = WordTree::Book.create(book_id, retrieved.metadata, retrieved.content)
  rescue Errno::ENOENT
    nil
  end
elsif options[:restrict_book_id]
  if $lib.nil?
    puts "library not set"
    exit -1
  end
  case_study = $lib.find_without_ngrams(options[:restrict_book_id])
end

worker = TextWorker.new(case_study ? case_study.content : nil, options[:refs])

content_q = Queue.new

ref_id = 0

start = Time.now

io_thread = Thread.new do
  if options[:library] == "-"
    $stdin.each_line do |path|
      ref_id += 1
      id = WordTree::Disk::LibraryLocator.id_from_path(path)
      content_q.push([id, path.strip, ref_id])
    end
  else
    $lib.library.each do |path, id|
      ref_id += 1
      content_q.push([id, path, ref_id])
    end
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

case options[:format].downcase
when "json" then
  meta =   {
    "_meta" => {
      "description" => "Baseline word frequencies for unigrams to 4-grams of words" + (case_study ? " for book #{case_study.id} (#{case_study.year})" : ""),
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
when "txt" then
  File.open(options[:output], "w") do |file|
    file.puts "_description Baseline word frequencies for 1-grams to #{options[:n]}-grams of words" + (case_study ? " for book #{case_study.id} (#{case_study.year})" : "")
    file.puts "_book_count #{ref_id}"
    file.puts "_book_range_start_year #{1750}"
    file.puts "_book_range_end_year #{1860}"
    file.puts "_processing_time_in_seconds #{finish - start}"
    file.puts "_processing_time_avg_per_book #{(finish - start).to_f / ref_id}"

    worker.trie.each do |k, v|
      file.puts "#{k} #{v}"
    end
  end
end

$t = worker.trie
IRB.start
