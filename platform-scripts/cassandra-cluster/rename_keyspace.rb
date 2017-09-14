
require 'fileutils'
require 'tmpdir'
require 'optparse'

options = {}
OptionParser.new do |opts|
  opts.banner =  "usage: rename_keyspace --from <current-name> --to <new-name>"

  opts.on("--from=name", "Current keyspace name") do |name|
    options[:from] = name
  end
  opts.on("--to=name", "New keyspace name") do |name|
    options[:to] = name
  end

  opts.on("--verbose", "Verbose output") do |v|
    options[:verbose] = v
  end

end.parse!

if options[:from] == nil or options[:to] == nil
  puts "from and to expected"
  exit(1)
end

dir = Dir.mktmpdir # temp working dir

from = options[:from]
to = options[:to]
keyspace_cmd_file = File.join(dir, "target_keyspace.cql")
keyspace_cmd =  `cqlsh -e "describe #{from};"`.gsub("KEYSPACE #{from}", "KEYSPACE #{to}").gsub("#{from}\.", "#{to}\.")
File.write(keyspace_cmd_file, keyspace_cmd)

puts "create keyspace: #{to} and tables"
output = `cqlsh -f #{keyspace_cmd_file}`
puts output if options[:verbose]

puts "exporting data from #{options[:from]}"
tables = `cqlsh -e "use #{options[:from]}; describe tables;"`.split(" ")
tables.each do |table|
  full_path = File.join(dir, options[:from], "#{table}.csv")
  FileUtils.mkdir_p(File.dirname(full_path))
  puts "Exporting #{options[:from]}.#{table} > #{full_path}"
  output = `cqlsh -e "copy  #{options[:from]}.#{table} to '#{full_path}'"`
  puts output if options[:verbose]

  puts "copy #{from}.#{table} to #{to}.#{table}"
  output = `cqlsh -e "copy  #{options[:to]}.#{table} from '#{full_path}'"`
  puts output if options[:verbose]
end

