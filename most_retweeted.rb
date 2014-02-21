starttime = Time.now.to_f

# Manually defined spam accounts that got into our set (replaces these with your own)
spam_accounts = []

if ARGV.size == 0
	puts "Usage: $ ruby script.rb [-o][-j][-t][-c 50] files_to_parse.json(can be an array) [> outputfile.csv]\n\nOptions:
	-o\tSkip diagnostic and performance messages so output can be directly stored
	-j\tFormat output as JSON for D3JS usage (nodes and integer links) instead of CSV
	-t\tSquelch results and only show time and statistics
	-c 50\tLimit output to the top N-many "


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
	if ARGV.include?"-c"
		deletelocation = toscan.index("-c")
		result_limit = toscan[deletelocation+1].to_i

		# Delete argument
		toscan.delete_at(deletelocation)

		# Delete the argument value as well
		toscan.delete_at(deletelocation)
	else
		result_limit = false
	end 


	if !forexport
		puts "Squelching output, running for time only" unless !timeonly
		puts "Importing #{toscan.size} files"
		puts "Limit to: #{result_limit} results" unless !result_limit
		puts "-----------------------------------"
	end

	require 'yaml'
	require 'oj'
	require 'json'

	linecount = 0
	badlines = 0
	spamskips = 0
	retweet_counts = Hash.new

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
					
					# If retweeter or original account are spam accounts, skip this insert, otherwise include it.
					if spam_accounts.include?(retweeter) or spam_accounts.include?(original_tweet)
						spamskips += 1
					else
						if retweet_counts[original_tweet] != nil
							# This user has been retweeted before
							retweet_counts[original_tweet] += 1
						else
							retweet_counts[original_tweet] = 1
						end
					end
				end
			rescue
				badlines += 1
				#puts "Bad line in file: #{filename}\n    at: #{linecount}"
			end
		end
		sourcedata.close
	end
	sorted = retweet_counts.sort {|a1,a2| a2[1].to_i <=> a1[1].to_i }
	if !jsonexport
		puts "Screen_Name,Count" unless timeonly
		if !result_limit
			sorted.each do |key, value|
				puts "#{key},#{value}" unless timeonly
			end
		else
			final_results = 0
			sorted.each do |key, value|
				if final_results <= result_limit 
					puts "#{key},#{value}" unless timeonly
				end
				final_results += 1
			end
		end
	else
		to_out = []
		if !result_limit
			sorted.each do |key, value|
				to_out << key
			end
		else
			final_results = 0
			sorted.each do |key, value|
				if final_results <= result_limit 
					to_out << key
				end
				final_results += 1
			end
		end
		puts to_out.to_s
	end


	# Only export stats if asked
	endtime = Time.now.to_f
	if !forexport
		puts "-----------------------------------"
		puts "Total files: #{ARGV.size}"
		puts "Processed: #{linecount} lines"
		puts "Couldn't read: #{badlines} lines"
		puts "Skpped: #{spamskips} (as spam)"
		puts "Duration: #{endtime-starttime} seconds"
		puts "-----------------------------------"
	end
end