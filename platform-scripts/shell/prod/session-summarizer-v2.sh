#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3
export JAVA_HOME=/usr/java/current

## Job to run daily
cd /mnt/data/analytics/scripts
endDate=$(date --date yesterday "+%Y-%m-%d")

sess_config='{"search":{"type":"s3","queries":[{"bucket":"prod-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"filters":[{"name":"ver","operator":"EQ","value":"2.0"}],"model":"org.ekstep.analytics.model.LearnerSessionSummarizerV2","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.207:9092","topic":"production.telemetry.derived"}}],"parallelization":8,"appName":"Learner Session Summarizer V2","deviceMapping":true}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerSessionSummarizerV2 /mnt/data/analytics/models/batch-models-1.0.jar --config "$sess_config" > "logs/$endDate-sess-summary-v2.log" 2>&1&