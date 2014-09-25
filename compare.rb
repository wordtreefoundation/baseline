require 'wordtriez'
require 'wordtree'
require 'optparse'
require 'byebug'

class TrieCache
  def initialize(ngrams=4, use_cache=true, chdir=nil)
    @ngrams = ngrams
    @use_cache = use_cache
    @maybe_chdir = chdir

    @trie_cache = {}
    @bookmeta_cache = {}
  end

  def book_metadata(book)
    {
      id: book.id,
      year: book.year,
      bytes: book.size_bytes,
      words: book.content.count(' ')
    }
  end

  def chdir_load(dir, file)
    if dir.nil?
      return Preamble.load(file, :external_encoding => "utf-8")
    else
      FileUtils.cd(dir) do
        return Preamble.load(file, :external_encoding => "utf-8")
      end
    end
  end

  def load_bookmeta_trie_text(key)
    trie = Wordtriez.new

    book_data = chdir_load(@maybe_chdir, key)
    book_id = WordTree::Disk::LibraryLocator.id_from_path(key)
    book = WordTree::Book.create(book_id, book_data.metadata, book_data.content.scrub)
    bookmeta = book_metadata(book)

    trie.add_text!(book.content, @ngrams)

    return [bookmeta, trie, book.content]
  end

  def get_with_text(key)
    if @use_cache
      if @trie_cache.has_key?(key)
        # Cool! We've already loaded it, save us some time
        # $stderr.puts "Using cache for #{key}"
        trie = @trie_cache.fetch(key)
        bookmeta = @bookmeta_cache.fetch(key)
      else
        # Load this time
        bookmeta, trie, text = load_bookmeta_trie_text(key)
        # Save it for later as well
        @bookmeta_cache[key] = bookmeta
        @trie_cache[key] = trie
      end
    else
      # Not allowed to use cache (memory is scarce?)
        bookmeta, trie, text = load_bookmeta_trie_text(key)
    end

    [bookmeta, trie, text]
  end

  def get(key)
    bookmeta, trie, text = get_with_text(key)
    [bookmeta, trie]
  end
end

options = {
  :files => "-",
  :ngrams => 4,
  :use_cache => true
}

OptionParser.new do |opts|
  opts.banner = "Usage: compare.rb [options]"

  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end

  opts.on("-f", "--files FILELIST", "Read files from FILELIST. Use '-' to take file paths from STDIN.") do |path|
    options[:files] = path
  end

  opts.on("-b", "--baseline BASELINE", "Read baseline from BASELINE (bz2 file)") do |path|
    options[:baseline] = path
  end

  opts.on("", "--limit-baseline UPPER", "Upper limit of keys to load from baseline (used for testing)") do |value|
    options[:limit_baseline] = Integer(value)
  end

  opts.on("-n", "--ngrams N", "Generate x-grams from 1 to N") do |n|
    options[:ngrams] = Integer(n)
  end

  opts.on("", "--chdir DIR", "Change working dir to DIR before processing") do |path|
    options[:chdir] = path
  end

  opts.on("", "--[no-]cache", "Do comparison in memory (faster, but all files must fit in memory)") do |bool|
    options[:use_cache] = bool
  end
end.parse!

baseline = Wordtriez.new
cache = TrieCache.new(options[:ngrams], options[:use_cache], options[:chdir])

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

count = 0
if options[:baseline]
  IO.popen("bzip2 -d -c #{options[:baseline]}") do |io|
    begin
      while line = io.readline
        key, value = line.rpartition(" ")
        baseline[key] = value.to_i
        count += 1
        puts count if (count % 1_000_000) == 0
        break if options[:limit_baseline] and count > options[:limit_baseline]
      end
    rescue EOFError
    end
  end
else
  book_paths.each do |path|
    $stderr.puts "Loading #{path}..."
    meta, trie, text = cache.get_with_text(path)
    baseline.add_text!(text, 4)
  end
end

$stderr.puts "Starting comparison (#{book_count} books) #{options[:use_cache] ? "in memory" : ""}..."

count = 1
book_paths.each do |path_x|
  begin
    mx, trie_x = cache.get(path_x)
  rescue => e
    $stderr.puts "error: #{e}"
    next
  end

  book_paths.each do |path_y|
    next if path_x == path_y

    begin
      my, trie_y = cache.get(path_y)
    rescue => e
      $stderr.puts "error: #{e}"
      next
    end

    sum = 0
    # puts path_y
    trie_y.each do |ngram, n_y|
      # puts "ngram: #{ngram} #{n_y}"
      n_x = trie_x[ngram]
      r = baseline[ngram]
      if r == 0
        # if we don't have any record of the ngram, make it insignificant
        if options[:verbose]
          $stderr.puts "'#{ngram}' not found in baseline, assuming insignificance"
        end
        r = book_count * 1_000_000
      end
      sum += Math.sqrt(n_x * n_y) / (r.to_f / book_count)
      if options[:verbose]
        $stderr.puts "'#{ngram}' n_y: #{n_y}, n_x: #{n_x}, r: #{r}, sum: #{sum}"
      end
    end

    score = sum / Math.sqrt(mx[:words] ** 2 + my[:words] ** 2 )

    puts [count, Time.now,
      mx[:id], mx[:year], mx[:bytes], mx[:words],
      my[:id], my[:year], my[:bytes], my[:words],
      sum, score
    ].join("\t")

    count += 1
  end
end
