#!/bin/bash

log_file=$1
#webhook_url="https://hooks.slack.com/services/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"

failed=`grep "logType\":\"FAILED" $log_file | head -n 1`
complete=`grep "logType\":\"COMPLETED" $log_file | head -n 1`

model=""
event_date=""
events=""
time_taken=""
job_status=""
message=""

if [ ! -z "$failed" -a "$failed" != "" ]; then
	model=`sed 's/.*model":"\(.*\)","ver.*/\1/' <<< "$failed"`
	job_status="FAILED"
	message=`sed 's/.*message":"\(.*\)","class.*/\1/' <<< "$failed"`
	event_date=`sed 's/.*"data":{"date":"\(.*\)"},"message":.*/\1/' <<< "$failed"`
fi

if [ ! -z "$complete" -a "$complete" != "" ]; then
	model=`sed 's/.*model":"\(.*\)","ver.*/\1/' <<< "$complete"`
	job_status="COMPLETED"
	message=`sed 's/.*message":"\(.*\)","class.*/\1/' <<< "$complete"`
	events=`sed 's/.*","events":\(.*\),"timeTaken".*/\1/' <<< "$complete"`
	time_taken=`sed 's/.*,"timeTaken":\(.*\)},"message":.*/\1/' <<< "$complete"`
	event_date=`sed 's/.*{"date":"\(.*\)","events":.*/\1/' <<< "$complete"`
fi

data='{"channel": "#test_webhooks", "username": "Monitoring-DP", "text":"*-:Job Run Status Report:-*\n *Model :* `'$model'` \n *Status :* `'$job_status'` \n *Total events :* '$events' \n *Time taken :* '$time_taken' seconds \n *Event Date :* `'$event_date'` \n *Message :* `'$message'`", "icon_emoji": ":sunglasses:"}'
curl -X POST -H 'Content-Type: application/json' --data "$data" https://hooks.slack.com/services/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