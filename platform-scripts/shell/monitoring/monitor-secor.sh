#!/bin/bash
log_file=$1

warn=`grep " WARN " $log_file`
errors=`grep " ERROR " $log_file`
upload=`grep " INFO  uploading file " $log_file`

warn_count=`echo "$warn" | wc -l`
upload_count=`echo "$upload" | wc -l`
errors_count="0"
if [ "$errors" != "" ]; then
	errors_count=`echo "$errors" | wc -l`
fi

today=$(date "+%Y-%m-%d")

## Upload Info
file_names=""
while read -r line
do
	f=`sed 's/.*s3n:\(.*\).*/\1/' <<< "$line"`
	file_names="$file_names s3n:$f\n"
done <<< "$upload"

## Errors
error_msg=""
while read -r line
do
	msg=`sed 's/.*ERROR \(.*\).*/\1/' <<< "$line"`
	error_msg="$error_msg$msg\n"
done <<< "$errors"

##Warnings
warn_msg=""
while read -r line
do
	msg=`sed 's/.*WARN \(.*\).*/\1/' <<< "$line"`
	warn_msg="$warn_msg$msg\n"
done <<< "$warn"

echo "Warn Count: $warn_count"
echo "Upload Count: $upload_count"
echo "Errors Count: $errors_count"
echo -e $file_names

msg="$warn_msg$error_msg"
echo -e $msg >> secor-monitor-$today.txt

data='{"channel": "#test_webhooks", "username": "Monitoring-Secor", "text":"*-:Secor Monitoring Status Report:-*\n *Number of Uploaded Files: * `'$upload_count'` \n *Number of Warnings: * `'$warn_count'` \n *Number of Errors: * `'$errors_count'` \n\n File Names:\n```'$file_names'```", "icon_emoji": ":sunglasses:"}'
curl -X POST -H 'Content-Type: application/json' --data "$data" https://hooks.slack.com/services/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