require 'wordtriez'
require 'wordtree'
require 'optparse'
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
  include Celluloid

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

$visor = Celluloid::SupervisionGroup.run!
$visor.pool(TextWorker, as: :text_workers, args: [case_study.content, options])

ref_id = 0
$lib.library.map do |path, id|
  ref_id += 1
  $visor[:text_workers].future.count_ngrams(id, path, ref_id)
end.map(&:value)

puts "Done."

IRB.start
