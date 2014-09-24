require 'wordtriez'
require 'optparse'

options = {
  :library => "library",
  :output => "results.txt",
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

  opts.on("-b", "--baseline BASELINE", "Read baseline from BASELINE (bz2 file)") do |path|
    options[:baseline] = path
  end

  opts.on("", "--limit-baseline UPPER", "Upper limit of keys to load from baseline (used for testing)") do |value|
    options[:limit_baseline] = Integer(value)
  end

  opts.on("-n", "--ngrams N", "Generate x-grams from 1 to N") do |n|
    options[:n] = Integer(n)
  end

  opts.on("", "--chdir DIR", "Change working dir to DIR before processing") do |path|
    options[:chdir] = path
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

if options[:chdir]
  $stderr.puts "Using #{options[:chdir]} as working dir"
  FileUtils.chdir(options[:chdir])
end

$stderr.puts "Waiting for book list from STDIN..."
book_paths = $stdin.each_line.map do |line|
  $stderr.print line
  line.strip
end

book_count = book_paths.size

$stderr.puts "Starting comparison (#{book_count} books)..."
book_paths.each do |path_x|
  trie_x = Wordtriez.new
  begin
    text_x = IO.read(path_x)
  rescue => e
    $stderr.puts "error: #{e}"
    next
  end
  words_x = text_x.count(' ')
  trie_x.add_text!(text_x, 4)
  book_paths.each do |path_y|
    trie_y = Wordtriez.new
    begin
      text_y = IO.read(path_y)
    rescue => e
      $stderr.puts "error: #{e}"
      next
    end

    print File.basename(path_x)
    print " "
    print File.basename(path_y)

    words_y = text_y.count(' ')
    trie_y.add_text!(text_y, 4)

    sum = 0
    trie_y.each do |ngram, n_y|
      n_x = trie_x[ngram]
      r = baseline[ngram]
      r = book_count if r == 0
      sum += Math.sqrt(n_x * n_y) / (r.to_f / book_count)
      if options[:verbose]
        $stderr.puts "#{ngram} n_y: #{n_y}, n_x: #{n_x}, r: #{r}, sum: #{sum}"
      end
    end

    score = sum / Math.sqrt(words_x ** 2 + words_y ** 2 )
    puts " #{score}"
  end
end
