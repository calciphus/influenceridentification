starttime = Time.now.to_f

# Manually defined spam accounts that got into our set (replaces these with your own)
spam_accounts = ['WoWFactz','buyrealvisitors','UnrevealedFacts','Fact','ESPNStatsInfo','Melodyku01', 'Alesia_Cestaro', 'FalyaAlzara', 'Melodyku01', 'Nisa_Maniezs']
prime_accounts = ["bill_nizzle", "HITpol", "CloudExpo", "WoWFactz", "KirkDBorne", "imbigdata", "Analytics_Edge", "TheNextWeb", "buyrealvisitors", "LowonganYogya", "BigDataBlogs", "kdnuggets", "IBMbigdata", "JohndeVoogd", "yochum", "LowonganKerjaID", "ThingsExpo", "smabhai", "SamElliott_NLP", "byJamesScott", "IBMAnalytics", "TheMobileAppBiz", "BigDataExpo", "BernardMarr", "LocalBlox", "googleanalytics", "TechCrunch", "socialmedia2day", "ESPNStatsInfo", "marcusborba", "SinghKSarvesh", "BigDataStartups", "MIB_India", "projecteve1", "BetaList", "BigDataNetwork", "GonzalezCarmen", "eric_kavanagh", "webseoanalytics", "BigDataClub", "TheUnRealTimes", "MariaJohnsenSEO", "KiranKS", "dr_morton", "hootsuite", "JobsWithAce", "UnrevealedFacts", "Zanikweb", "KISSmetrics", "igrigorik", "TARGIT", "DataSift", "DevOpsSummit", "mjcavaretta", "SmartDataCo", "strataconf", "SAPAnalytics", "EdwardTufte", "icrunchdata", "M_Lekhi", "VishalTx", "FastCompany", "ForbesTech", "OracleAnalytics", "HarvardBiz", "BradfordBrown", "twitterapi", "BigDataNewsco", "github", "bobehayes", "MSFT4Work", "rwang0", "Doug_Laney", "SDDCexpo", "mediacrooks", "TIBCO", "simonlporter", "Real_Ashok", "danbarker", "bigdata", "BigDataGal", "Fact", "SmarterPlanet", "NLP_LenaSchell", "avinash", "MDMGeek", "fgautier26", "DataIsFuture", "mikko", "tableau", "GilPress", "SocialservicNet", "Oracle", "hollingsworth", "mappingBIGdata", "BrandBizy", "IBMSoftware", "gigaom", "Accenture", "bulkbits", "aantonop"]
if ARGV.size == 0
	puts "Usage: $ ruby script.rb [-o][-j][-t] files_to_parse.json(can be an array) [> outputfile.csv]\n\nOptions:
	-o\tSkip diagnostic and performance messages so output can be directly stored
	-b\tBrief output - exclude anyone who only shows up once, only works with -j
	-j\tFormat output as JSON for D3JS usage (nodes and integer links) instead of CSV
	-t\tSquelch results and only show time and statistics"

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
	if ARGV.include?"-b"
		makeitbrief = true
		namearray = []
		toscan.delete("-b")
	else
		makeitbrief = false
		namearray = []
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
	spamskips = 0
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
					
					# If retweeter or original account are spam accounts, skip this insert, otherwise include it.
					if spam_accounts.include?(retweeter) or spam_accounts.include?(original_tweet)
						spamskips += 1
					else
						namearray << retweeter
						namearray << original_tweet
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
				end
			rescue
				badlines += 1
				#puts "Bad line in file: #{filename}\n    at: #{linecount}"
			end
		end
		sourcedata.close
	end
	if !jsonexport
		puts "Tweeter,Re-tweeter,Link Count,Source Weight" unless timeonly
		retweeted_by.each do |key, array|
			# Only show people who have been retweeted more than twice, to cut down sample size
			if array.size > 2	
				array.each do |akey, value|
					# Original User's name, Retweet user's name, number of retweets
					puts "#{key},#{akey},#{value},#{array.size}" unless timeonly
				end
			end
		end
	else
		# Gather all IDs, make unique and put into an array so we can have an ID lookup
		id_array = []
		retweeted_by.each do |key, array|
			# Only show people who have been retweeted more than twice, to cut down sample size
			if array.size > 2	
				array.each do |akey, value|
					id_array << key
					id_array << akey
				end
			end
			# Remove duplicate names
			id_array = id_array.uniq	
		end
		
		# Split namearray into two arrays, one which has everyone who only appears once, one which has everyone who appears more than once
		stat = namearray.inject(Hash.new(0)) do |h,e|
			h[e] += 1
			h
		end
		onename, multiname = stat.partition {|e,c| c == 1}
		onename = onename.map! {|e,c| e}
		multiname = multiname.map! {|e,c| e}
		
		# Ouput nodes as JSON array
		nodes = []
		
		if makeitbrief
			multiname.each do |ia|
				if prime_accounts.include?ia
					nodes << Hash("name" => ia, "group" => 0)
				else
					if multiname.include?ia
						nodes << Hash("name" => ia, "group" => 1)
					elsif !makeitbrief
						nodes << Hash("name" => ia, "group" => 2)			
					end
				end
			end
		else
			id_array.each do |ia|
				if prime_accounts.include?ia
					nodes << Hash("name" => ia, "group" => 0)
				else
					if multiname.include?ia
						nodes << Hash("name" => ia, "group" => 1)
					elsif !makeitbrief
						nodes << Hash("name" => ia, "group" => 2)			
					end
				end
			end

		end
		# Output links as JSON array
		links = []
		retweeted_by.each do |key, array|
			# Only show people who have been retweeted more than 10x, to cut down sample size
			if array.size > 2	
				array.each do |akey, value|
					# Original User's name, Retweet user's name, number of retweets
					if makeitbrief
						# Only add if retweeter shows up more than once
						if multiname.include?akey
							# Index of source
							sindex = multiname.index(key)
							# Index of target
							tindex = multiname.index(akey)
							hash = Hash("source" => sindex.to_i,"target"=> tindex.to_i, "value" => array.size)
							if hash
								links << hash
							end
						end
					else
						# Index of source
						sindex = id_array.index(key)

						# Index of target
						tindex = id_array.index(akey)
						hash = Hash("source" => sindex.to_i,"target"=> tindex.to_i, "value" => array.size)
						if hash
							links << hash
						end
					end
				end
			end
		end

		d3js_formatted = Hash("nodes" => nodes, "links" => links)
		puts d3js_formatted.to_json unless timeonly
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