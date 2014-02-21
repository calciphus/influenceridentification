starttime = Time.now.to_f
# Take an argument of -o to suqelch all the extra reporting.

if ARGV.size == 0
	puts "Usage: $ ruby script.rb [-o][-j][-t] files_to_parse.json(can be an array) [> outputfile.csv]\n\nOptions:
	-o\tSkip diagnostic messages and format for storage into file
	-j\tFormat output as JSON instead of CSV
	-t\tSquelch results and only show time and counts"

else
	toscan = ARGV
	if ARGV.include?"-o"
		forexport = true
		toscan.delete("-o")
	else
		forexport = false
	end
	if ARGV.include?"-j"
		jsonexport = true
		toscan.delete("-j")
	else
		jsonexport = false
	end
	if ARGV.include?"-t"
		timeonly = true
		toscan.delete("-t")
	else
		timeonly = false
	end 

	if !forexport
		puts "Squelching output, running for time only" unless !timeonly
		puts "Importing #{ARGV.size} files"
		puts "-----------------------------------"

	end

	require 'yaml'
	require 'oj'
	require 'json'

	linecount = 0
	badlines = 0
	retweeted_by = Hash.new


	toscan.each do |filename|

		sourcedata = File.open(filename).each_line do |line|
			linecount += 1
			begin
				linearray = Oj.load(line)
				rtc = linearray["twitter"]["retweet"] rescue nil
				if rtc != nil
					# Retweeter's screenname
					retweeter = linearray['twitter']['retweet']['user']['screen_name']
					# Original tweeter's screenname
					original_tweet = linearray['twitter']['retweeted']['user']['screen_name']
					if retweeted_by[original_tweet] != nil
						# This user has been retweeted before
						if retweeted_by[original_tweet][retweeter] != nil
							# This user has been retweeted by this retweeter before
							retweeted_by[original_tweet][retweeter] += 1
						else
							retweeted_by[original_tweet][retweeter] = 1
						end
					else
						# Create a new hash and store in master hash
						retweet_hash = Hash.new
						retweet_hash[retweeter] = 1
						retweeted_by[original_tweet] = retweet_hash
					end
				end
			rescue
				badlines += 1
				#puts "Bad line in file: #{filename}\n    at: #{linecount}"
			end
		end
		sourcedata.close
	end
	if !jsonexport
		puts "Tweeter,Source Weight" unless timeonly
		retweeted_by.each do |key, array|
			# Only show people who have been retweeted more than twice, to cut down sample size
			if array.size > 5	
				puts "#{key},#{array.size}" unless timeonly
			end
		end
	else
		puts retweeted_by.to_json unless timeonly
	end
	# Only export stats if asked
	endtime = Time.now.to_f
	if !forexport
		puts "-----------------------------------"
		puts "Total files: #{ARGV.size}"
		puts "Processed: #{linecount} lines"
		puts "Couldn't read: #{badlines} lines"
		puts "Duration: #{endtime-starttime} seconds"
		puts "-----------------------------------"
	end
end