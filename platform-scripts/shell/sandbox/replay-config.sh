#!/usr/bin/env bash

config() {
	case "$1" in
	   "ss") echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.LearnerSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Learner Session Summarizer","deviceMapping":true}'
	   ;;
	   "gls") echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.GenieLaunchSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Genie Launch Summarizer","deviceMapping":false}'
	   ;;
	   "gss") echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.GenieUsageSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Genie Session Summarizer","deviceMapping":false}'
	   ;;
	   "cus") echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"ss/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.ContentUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'
	   ;;
	   "css") echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.ContentSideloadingSummarizer","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"ContentSideloadingSummarizer","deviceMapping":false}'
	   ;;
	   "dus") echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"gls/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.DeviceUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"DeviceUsageSummary","deviceMapping":false}'
	   ;;
	   "cuu") echo '{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"cus/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.updater.ContentUsageUpdater","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Content Usage Updater","deviceMapping":false}'
	   ;;
	   *)  
		echo "Unknown model code" 
      	exit 1 # Command to come out of the program with status 1
      	;;
	esac
}