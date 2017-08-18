#!/usr/bin/env bash

config() {
	bucket=ekstep-dev-data-store
	datasetRawBucket=ekstep-data-sets-dev
	dataExhaustBucket=ekstep-public-dev
	dataExhaustPrefix=data-exhaust/
	consumptionRawPrefix=datasets/D001/4208ab995984d222b59299e5103d350a842d8d41/
	brokerList=localhost:9092
	topic=local.telemetry.derived
	currentDate=$(date "+%Y-%m-%d");
	temp_folder=transient-data
	if [ -z "$2" ]; then endDate=$(date -v -1d "+%Y-%m-%d"); else endDate=$2; fi
	case "$1" in
	   	"ss")
		echo '{"search":{"type":"s3","queries":[{"bucket":"'$bucket'","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.LearnerSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"s3","params":{"bucket": "'$bucket'","key": "'$temp_folder'/ss/'$endDate'","bucket": "'$bucket'"}},{"to":"kafka","params":{"brokerList":"'$brokerList'","topic":"'$topic'"}}],"parallelization":8,"appName":"Learner Session Summarizer","deviceMapping":true}'
	   	;;
	   	"cus")
		echo '{"search":{"type":"s3","queries":[{"bucket":"'$bucket'","prefix":"'$temp_folder'/ss/","endDate":"'$endDate'","delta":0,"folder":"true"}]},"model":"org.ekstep.analytics.model.ContentUsageSummary","output":[{"to":"s3","params":{"bucket": "'$bucket'","key": "'$temp_folder'/cus/'$endDate'","bucket": "'$bucket'"}},{"to":"kafka","params":{"brokerList":"'$brokerList'","topic":"'$topic'"}}],"parallelization":10,"appName":"Content Usage Summarizer","deviceMapping":false}'
		;;
		"*")
		echo "Unknown model code"
      	exit 1 # Command to come out of the program with status 1
      	;;
	esac
}
