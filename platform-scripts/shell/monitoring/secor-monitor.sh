#!/bin/bash
log_file=$1
warn=`grep " WARN " $log_file`
upload=`grep " INFO  uploading file " $log_file`

warn_count=`echo "$warn" | wc -l`
upload_count=`echo "$upload" | wc -l`

echo "warn count: $warn_count"
echo "upload count $upload_count"

msg=`sed 's/.*WARN \(.*\).*/\1/' <<< "$warn"`
echo -e $msg >> warnings.txt

data='{"channel": "#test_webhooks", "username": "Monitoring-Secor", "text":"*-:Secor Monitoring Status Report:-*\n *Number of Uploaded Files: * `'$upload_count'` \n *Number of Warnings: * `'$warn_count'`", "icon_emoji": ":sunglasses:"}'
curl -X POST -H 'Content-Type: application/json' --data "$data" https://hooks.slack.com/services/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