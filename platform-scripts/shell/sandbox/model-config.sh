#!/usr/bin/env bash

config() {
	case "$1" in
	   	"ss") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.LearnerSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Learner Session Summarizer","deviceMapping":true}'
	   	;;
	   	"gls") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.GenieLaunchSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Genie Launch Summarizer","deviceMapping":false}'
		;;
		"gss") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.GenieUsageSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Genie Session Summarizer","deviceMapping":false}'
		;;
		"cus") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ContentUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'
		;;
		"css") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ContentSideloadingSummarizer","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"ContentSideloadingSummarizer","deviceMapping":false}'
		;;
		"dus") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"gls/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.DeviceUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"DeviceUsageSummary","deviceMapping":false}'
		;;
		"cuu") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"cus/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.updater.ContentUsageUpdater","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Content Usage Updater","deviceMapping":false}'
		;;
		"as") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"filters":[{"name":"ver","operator":"EQ","value":"1.0"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":true}'
		;;
		"las") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":6}]},"model":"org.ekstep.analytics.model.LearnerActivitySummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"Learner Activity Summary","deviceMapping":false}'
		;;
		"ls") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"las/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.UpdateLearnerActivity","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Learner Snapshot","deviceMapping":false}'
		;;
		"lcas") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":6}]},"model":"org.ekstep.analytics.updater.LearnerContentActivitySummary","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Learner Content Activity Summary","deviceMapping":false}'
		;;
		"lp") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.ProficiencyUpdater","modelParams":{"alpha":1.0,"beta":1.0},"output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'
		;;
		"lcr") 
		endDate = if [ $2 == "" ]; then $(date --date yesterday "+%Y-%m-%d"); else $2; fi
		echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.RecommendationEngine","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'
		;;
		*)  
		echo "Unknown model code" 
      	exit 1 # Command to come out of the program with status 1
      	;;
	esac
}