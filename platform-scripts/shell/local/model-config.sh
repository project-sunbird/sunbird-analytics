#!/usr/bin/env bash

config() {
	#if [ -z "$2" ]; then endDate=$(date --date yesterday "+%Y-%m-%d"); else endDate=$2; fi
	if [ -z "$2" ]; then endDate=$(date -v -1d "+%Y-%m-%d"); else endDate=$2; fi
	case "$1" in
	   	"ss") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.LearnerSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":8,"appName":"Learner Session Summarizer","deviceMapping":true}'
	   	;;
	   	"is")
	   	echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ItemSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":8,"appName":"Item Summarizer","deviceMapping":false}'
	   	;;
	   	"dcus") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.DeviceContentUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":10,"appName":"DeviceContentUsageSummary","deviceMapping":false}'
		;;
	   	"gls") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.GenieLaunchSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":8,"appName":"Genie Launch Summarizer","deviceMapping":false}'
		;;
		"gss") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.GenieUsageSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":8,"appName":"Genie Session Summarizer","deviceMapping":false}'
		;;
		"cus") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ContentUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'
		;;
		"css") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ContentSideloadingSummarizer","output":[{"to":"console","params":{"printEvent":false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":10,"appName":"ContentSideloadingSummarizer","deviceMapping":false}'
		;;
		"dus") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"gls/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.DeviceUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":10,"appName":"DeviceUsageSummary","deviceMapping":false}'
		;;
		"cuu") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"cus/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.updater.ContentUsageUpdater","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Content Usage Updater","deviceMapping":false}'
		;;
		"as") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"filters":[{"name":"ver","operator":"EQ","value":"1.0"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":true}'
		;;
		"las") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":6}]},"model":"org.ekstep.analytics.model.LearnerActivitySummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":10,"appName":"Learner Activity Summary","deviceMapping":false}'
		;;
		"ls") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"las/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.UpdateLearnerActivity","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Learner Snapshot","deviceMapping":false}'
		;;
		"lcas") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":6}]},"model":"org.ekstep.analytics.updater.LearnerContentActivitySummary","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Learner Content Activity Summary","deviceMapping":false}'
		;;
		"lp") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ProficiencyUpdater","modelParams":{"alpha":1.0,"beta":1.0},"output":[{"to":"console","params":{"printEvent":false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'
		;;
		"dsu")
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.DeviceSpecification","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Device Specification Updater","deviceMapping":false}'
		;;
		"lcr") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.RecommendationEngine","output":[{"to":"console","params":{"printEvent":false}},{"to":"file","params":{"file":"telemetry-derived.log"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'
		;;
		"ctv") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ContentVectorsModel","modelParams":{"content2vec.s3_bucket":"sandbox-data-store","content2vec.s3_key_prefix":"model/","python.home":"/usr/local/bin/","content2vec.model_path":"/tmp/content2vec/model/","content2vec.kafka_topic":"sandbox.learning.graph.events","content2vec.kafka_broker_list":"172.31.1.92:9092","content2vec.corpus_path":"/tmp/content2vec/content_corpus/","content2vec.download_path":"/tmp/content2vec/download/","content2vec.search_request":{"request":{"filters":{"objectType":["Content"],"contentType":["Story","Worksheet","Collection","Game"],"status":["Live"]},"limit":100}}},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"ContentVectorsModel","deviceMapping":false}'
		;;
		"device-recos") 
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"dus/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.DeviceRecommendationModel","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"DeviceRecommendationModel","deviceMapping":false}'
		;;
		*)  
		echo "Unknown model code" 
      	exit 1 # Command to come out of the program with status 1
      	;;
	esac
}