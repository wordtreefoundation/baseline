require 'wordtree'
require 'wordtriez'
require 'optparse'
require 'thread'
require 'irb'
require 'json'
require 'melisa'
require 'fileutils'

class TextWorker
  attr_reader :trie, :book_ids

  def initialize(maybe_chdir=nil, source_text=nil, add_refs=false, n=4)
    @trie = Wordtriez.new
    @add_refs = add_refs
    @n = n
    @maybe_chdir = maybe_chdir
    @book_ids = {}
    if source_text
      @trie.add_text!(source_text, @n)
      # Restrict keys of subsequent books if we have a source_text
      @restrict_keys = true
    else
      @restrict_keys = false
    end
  end

  def chdir_load(path)
    if @maybe_chdir.nil?
      return File.open(path, "r:UTF-8", &:read)
    else
      FileUtils.cd(@maybe_chdir) do
        return File.open(path, "r:UTF-8", &:read)
      end
    end
  end

  def packed_symbolic_string(str)
    [str.to_sym.object_id].pack('N')
  end

  def count_ngrams(book_id, path, ref_id)
    puts "#{ref_id}: Processing #{book_id}..."
    text = chdir_load(path)
    puts "#{ref_id}: loaded from disk (#{text.size}b)"
    begin
      id = packed_symbolic_string(book_id)
      @book_ids[book_id.to_sym.object_id] ||= book_id
      args = [text, @n, @add_refs ? '-' + id : '']
      @trie.send(@restrict_keys ? :"union_text!" : :"add_text!", *args)
    rescue StandardError => e
      puts "#{ref_id}: **ERROR** while adding text: #{text ? text[0..100] : "nil"}... #{e}"
      raise e
    end
    Time.now.tap do |now|
      puts "#{ref_id}: added (#{now - $start} seconds since start - #{(now - $start).to_f / ref_id} avg), new size: #{@trie.size}"
    end
  end
end


options = {
  :refs => false,
  :output => "baseline.txt",
  :n => 4
}

OptionParser.new do |opts|
  opts.banner = "Usage: baseline.rb [options]"

  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end

  opts.on("-f", "--files FILELIST", "Read files from FILELIST. Use '-' to take file paths from STDIN.") do |path|
    options[:files] = path
  end

  opts.on("-o", "--output BASELINE", "Set baseline file path to BASELINE") do |path|
    options[:output] = path
  end

  opts.on("-r", "--restrict FILE", "Restrict baseline to ngrams found in text file FILE") do |path|
    options[:restrict] = path
  end

  opts.on("", "--format FORMAT", "Output format: 'txt' or 'json'") do |format|
    options[:format] = format
  end

  opts.on("-n", "--ngrams N", "Generate x-grams from 1 to N") do |n|
    options[:n] = Integer(n)
  end

  opts.on("", "--[no-]refs", "Include references to books (uses a lot of memory)") do |bool|
    options[:refs] = bool
  end

  opts.on("", "--chdir DIR", "Change working dir to DIR before processing") do |path|
    options[:chdir] = path
  end
end.parse!

if options[:output]
  options[:format] ||= File.extname(options[:output]).sub(/^\.+/, '')
end

get_book_paths = -> do
  if options[:files] == "-"
    $stderr.puts "Waiting for book list from STDIN..."
    files_file = $stdin
  else
    files_file = File.open(options[:files], "r")
  end
  files_file.each_line.map do |line|
    $stderr.print line
    line.strip
  end
end

book_paths = get_book_paths.call
book_count = book_paths.size

$start = Time.now

if options[:restrict]
  book_id = WordTree::Disk::LibraryLocator.id_from_path(options[:restrict])
  begin
    retrieved = Preamble.load(options[:restrict], :external_encoding => "utf-8")
    case_study = WordTree::Book.create(book_id, retrieved.metadata, retrieved.content)
  rescue Errno::ENOENT
    nil
  end
end

worker = TextWorker.new(options[:chdir], case_study ? case_study.content : nil, options[:refs], options[:n])

content_q = Queue.new

ref_id = 0

start = Time.now

io_thread = Thread.new do
  puts "Reading #{book_paths.size} books..."
  for path in book_paths
    id = WordTree::Disk::LibraryLocator.id_from_path(path)
    content_q.push([id, path.strip, ref_id+1])
    ref_id += 1
  end
end

work_thread = Thread.new do
  sleep 0.01 while ref_id == 0
  begin
    while args = content_q.pop(true)
      worker.count_ngrams(*args)    
    end
  rescue ThreadError => e
    $stderr.puts e
  end
end

io_thread.join
work_thread.join

finish = Time.now

puts "Done."


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
index = {
  "_index" => worker.book_ids
}

case options[:format].downcase
when "dic" then
  if options[:refs]
    raise "TODO: save refs to Marisa dictionary file"
  else
    mt = Melisa::IntTrie.new
    puts "Converting HAT-trie to Marisa-Trie..."
    worker.trie.each do |k, v|
      mt[k] = v
    end
    puts "Saving Marisa-Trie..."
    mt.save(options[:output])
  end
  puts "Saving metadata..."
  File.open(options[:output] + ".json", "w") do |file|
    file.write({"_meta" => meta, "_index" => index}.to_json)
  end
when "json" then
  File.open(options[:output], "w") do |file|
    file.puts "{"
    file.write JSON.pretty_generate(meta).split("\n").to_a[1..-2].join("\n") + ",\n"
    file.write JSON.pretty_generate(index).split("\n").to_a[1..-2].join("\n") + ",\n"
    worker.trie.each do |k, v|
      if options[:refs]
        ngram, id = k.force_encoding("binary").split('-')
        id = id.unpack('N').first
        file.puts("  \"#{ngram}\":[#{v},#{id}],")
      else
        file.puts "  \"#{k}\": #{v},"
      end
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

    worker.book_ids.each do |k, v|
      file.puts "_ #{k} #{v}"
    end

    count = 0
    worker.trie.each do |k, v|
      puts count if count % 1000 == 0
      if options[:refs]
        ngram, id = k.force_encoding("binary").split('-')
        id = id.unpack('N').first
        file.puts("#{v} #{id} #{ngram}")
      else
        file.puts "#{v} #{k}"
      end
      count += 1
    end
  end
end

# $t = worker.trie
# IRB.start
