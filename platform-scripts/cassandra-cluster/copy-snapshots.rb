require 'fileutils'

if ARGV.size < 3
	puts "usage: program datadir snapshotname targetdir"
	exit(1)
end

ignorelist = ["system", "system_auth", "system_traces", "system_schema", "system_distributed"]

datadir = ARGV[0]
snapshot = ARGV[1]
targetdir = ARGV[2]

FileUtils.rm_rf targetdir if File.exists?(targetdir)
FileUtils.mkdir_p targetdir

snapshot_paths = []
Dir.glob("#{datadir}/**/#{snapshot}").each do |p|
	cleaned_path = p.gsub(datadir, "").split("/").reject {|i| i == ""}
	keyspace = cleaned_path[0]
	table = cleaned_path[1]
	targetpath = File.join(targetdir, keyspace, table)
	if !ignorelist.include?(keyspace)
		FileUtils.mkdir_p targetpath
		puts "Copying to #{targetpath}"
		files = Dir.glob("#{p}/**/*")
		files.each do |f|
			puts "copy: #{f}"
			FileUtils.cp f, File.join(targetpath, File.basename(f))
		end
	end
end
