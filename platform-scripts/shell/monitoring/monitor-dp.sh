#!/bin/bash

log_file=$1
#webhook_url="https://hooks.slack.com/services/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"

failed=`grep "logType\":\"FAILED" $log_file`
complete=`grep "logType\":\"COMPLETED" $log_file`

succ_num=`echo "$complete" | wc -l`
total_events="0"
total_time="0"

model=""
event_date=""
events=""
time_taken=""
job_status=""
message=""

file_content="MODEL,JOB_STATUS,EVENTS_GENERATED,TIME_TAKEN(in Seconds),EVENT_DATE,MESSAGE\n"

while read -r line
do
    model=`sed 's/.*model":"\(.*\)","ver.*/\1/' <<< "$line"`
	job_status="COMPLETED"
	message=`sed 's/.*message":"\(.*\)","class.*/\1/' <<< "$line"`
	
	events=`sed 's/.*","events":\(.*\),"timeTaken".*/\1/' <<< "$line"`
	total_events=`echo $((total_events + events))`
	
	time_taken=`sed 's/.*,"timeTaken":\(.*\)},"message":.*/\1/' <<< "$line"`
	total_time=`echo $total_time + $time_taken | bc`
	
	event_date=`sed 's/.*{"date":"\(.*\)","events":.*/\1/' <<< "$line"`
	file_content+="$model,$job_status,$events,$time_taken,$event_date,$message\n"
done <<< "$complete"

model=""
event_date=""
events=""
time_taken=""
job_status=""
message=""

model_prv=""
failed_num="0"
while read -r line
do
	model=`sed 's/.*model":"\(.*\)","ver.*/\1/' <<< "$line"`
	if [ "$model" != "$model_prv" ]; then
		model_prv=$model
		job_status="FAILED"
		message=`sed 's/.*message":"\(.*\)","class.*/\1/' <<< "$line"`
		event_date=`sed 's/.*"data":{"date":"\(.*\)"},"message":.*/\1/' <<< "$line"`
		file_content+="$model,$job_status,$events,$time_taken,$event_date,$message\n"
		failed_num=`echo $((failed_num + 1))`
	fi
done <<< "$failed"

echo -e $file_content >> status_report.csv

echo "-------- Status Report --------"
echo "Number of Completed Jobs: $succ_num"
echo "Number of failed Jobs: $failed_num"
echo "Total time taken: $total_time"
echo "Total events generated: $total_events"

data='{"channel": "#test_webhooks", "username": "Monitoring-DP", "text":"*-:Job Run Status Report:-*\n *Number of Completed Jobs: * `'$succ_num'` \n *Number of failed Jobs: * `'$failed_num'` \n *Total time taken: * `'$total_time'` \n *Total events generated: * `'$total_events'`", "icon_emoji": ":sunglasses:"}'
curl -X POST -H 'Content-Type: application/json' --data "$data" https://hooks.slack.com/services/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