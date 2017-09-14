require 'fileutils'

if ARGV.size < 2
	puts "usage: program cassandra_host snapshot_dir"
	exit(1)
end

cassandra_host = ARGV[0]
snapshot_dir = ARGV[1]

Dir.glob("#{snapshot_dir}/**/*").select {|i| File.directory?(i)}.each do |d|
	full_path = File.expand_path(d)
	puts "restore: sstableloader -v -d #{cassandra_host} #{full_path}"
	system("sstableloader", "-v", "-d", cassandra_host, full_path)
end
