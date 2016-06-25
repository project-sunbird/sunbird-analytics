#!/bin/bash

log_file=$1
#webhook_url="https://hooks.slack.com/services/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"

failed_rows=`grep "logType\":\"FAILED" $log_file`
complete_rows=`grep "logType\":\"COMPLETED" $log_file`

today=$(date "+%Y-%m-%d")
failed_num=0
succ_num=`grep "logType\":\"COMPLETED" $log_file | wc -l | bc`
total_events=0
total_time=0

file_content="Model,Job Status,Events Count,Time Taken(in Seconds),Date,Message\n"

while read -r line
do	
    model=`sed 's/.*model":"\(.*\)","ver.*/\1/' <<< "$line" | sed -e "s/org.ekstep.analytics.model.//g"`
	job_status="COMPLETED"
	message=`sed 's/.*message":"\(.*\)","class.*/\1/' <<< "$line"`
	
	events=`sed 's/.*","events":\(.*\),"timeTaken".*/\1/' <<< "$line"`
	total_events=$((total_events + events))
	
	time_taken=`sed 's/.*,"timeTaken":\(.*\)},"message":.*/\1/' <<< "$line"`
	total_time=`echo $total_time + $time_taken | bc`

	
	event_date=`sed 's/.*{"date":"\(.*\)","events":.*/\1/' <<< "$line"`
	file_content+="$model,$job_status,$events,$time_taken,$event_date,$message\n"
done <<< "$complete_rows"

events=0
time_taken=0

while read -r line
do
	model=`sed 's/.*model":"\(.*\)","ver.*/\1/' <<< "$line" | sed -e "s/org.ekstep.analytics.model.//g"`
	if [ "$model" != "$model_prv" ]; then
		model_prv=$model
		job_status="FAILED"
		message=`sed 's/.*localizedMessage":"\(.*\)","message.*/\1/' <<< "$line"`
		event_date=`sed 's/.*"data":{"date":"\(.*\)"},"message":.*/\1/' <<< "$line"`
		file_content+="$model,$job_status,$events,$time_taken,$event_date,$message\n"
		failed_num=$((failed_num + 1))
	fi
done <<< "$failed_rows"

echo -e $file_content > status_report_$today.csv

echo "-------- Status Report --------"
echo "Number of Completed Jobs: $succ_num"
echo "Number of failed Jobs: $failed_num"
echo "Total time taken: $total_time"
echo "Total events generated: $total_events"

data='{"channel": "#analytics_monitoring", "username": "dp-monitor", "text":"*Job Run Status Report for '$today'*\nNumber of Completed Jobs: `'$succ_num'` \nNumber of failed Jobs: `'$failed_num'` \nTotal time taken: `'$total_time'`\nTotal events generated: `'$total_events'`\n\nDetailed Report:\n```'$file_content'```", "icon_emoji": ":ghost:"}'
curl -X POST -H 'Content-Type: application/json' --data "$data" https://hooks.slack.com/services/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