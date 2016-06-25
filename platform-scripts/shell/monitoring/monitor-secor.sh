#!/bin/bash
log_file=$1
mType=$2

warn=`grep " WARN " $log_file`
errors=`grep " ERROR " $log_file`
upload=`grep " INFO  uploading file " $log_file`

warn_count=`echo "$warn" | wc -l | bc`
upload_count=`echo "$upload" | wc -l | bc`
errors_count=0
if [ "$errors" != "" ]; then
	errors_count=`echo "$errors" | wc -l | bc`
fi

echo "Warn Count: $warn_count"
echo "Upload Count: $upload_count"
echo "Errors Count: $errors_count"

today=$(date "+%Y-%m-%d")

## Upload Info
file_names=""
while read -r line
do
	f=`sed 's/.*s3n:\(.*\).*/\1/' <<< "$line"`
	file_names+="s3:$f\n"
done <<< "$upload"

echo "## Logging errors "
file_content="Type,Message\n"
## Errors
while read -r line
do
	msg=`sed 's/.*ERROR \(.*\).*/\1/' <<< "$line"`
	file_content+="ERROR,$msg\n"
done <<< "$errors"
echo "## errors logged "

echo "## Logging warning "
##Warnings
while read -r line
do
	msg=`sed 's/.*WARN \(.*\).*/\1/' <<< "$line"`
	file_content+="WARN,$msg\n"
done <<< "$warn"
echo "## warnings logged "

echo -e $file_content > secor-monitor-$today.csv

data='{"channel": "#analytics_monitoring", "username": "secor-monitor", "text":"*Secor | Monitoring Report | '$today' | '$mType'*\nFiles Uploaded: `'$upload_count'` \n Warnings: `'$warn_count'` \n Errors: `'$errors_count'` \n\nUploaded Files:\n\n```'$file_names'```", "icon_emoji": ":ghost:"}'
#echo $data
curl -X POST -H 'Content-Type: application/json' --data "$data" https://hooks.slack.com/services/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