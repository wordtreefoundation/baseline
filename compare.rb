require 'wordtriez'
require 'optparse'
require 'byebug'

def chdir_read(dir, file)
  if dir.nil?
    return File.open(file,'r:UTF-8',&:read).scrub
  else
    FileUtils.cd(dir) do
      return File.open(file,'r:UTF-8',&:read).scrub
    end
  end
end

class TrieCache
  def initialize(ngrams=4, use_cache=true, chdir=nil)
    @ngrams = ngrams
    @use_cache = use_cache
    @maybe_chdir = chdir

    @trie_cache = {}
    @wordcount_cache = {}
  end

  def load_trie_and_wordcount(key)
    trie = Wordtriez.new
    text = chdir_read(@maybe_chdir, key)

    # could improve the accuracy of wordcount if we counted *after*
    # add_text! but because Wordtriez adds null char, ruby doesn't like that
    wordcount = text.count(' ')
    trie.add_text!(text, @ngrams)

    return [trie, wordcount]
  end

  def get(key)
    if @use_cache
      if @trie_cache.has_key?(key)
        # Cool! We've already loaded it, save us some time
        # $stderr.puts "Using cache for #{key}"
        trie = @trie_cache.fetch(key)
        wordcount = @wordcount_cache.fetch(key)
      else
        # Load this time
        trie, wordcount = load_trie_and_wordcount(key)
        # Save it for later as well
        @trie_cache[key] = trie
        @wordcount_cache[key] = wordcount
      end
    else
      # Not allowed to use cache (memory is scarce?)
      trie, wordcount = load_trie_and_wordcount(key)
    end

    [trie, wordcount]
  end
end

options = {
  :files => "-",
  :output => "results.txt",
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

count = 0
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
cache = TrieCache.new(options[:ngrams], options[:use_cache], options[:chdir])

$stderr.puts "Starting comparison (#{book_count} books) #{options[:use_cache] ? "in memory" : ""}..."

book_paths.each do |path_x|
  begin
    trie_x, words_x = cache.get(path_x)
  rescue => e
    $stderr.puts "error: #{e}"
    next
  end

  book_paths.each do |path_y|
    next if path_x == path_y

    begin
      trie_y, words_y = cache.get(path_y)
    rescue => e
      $stderr.puts "error: #{e}"
      next
    end

    print File.basename(path_x)
    print " "
    print File.basename(path_y)

    sum = 0
    trie_y.each do |ngram, n_y|
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

    score = sum / Math.sqrt(words_x ** 2 + words_y ** 2 )
    puts " #{score}"
  end
end
