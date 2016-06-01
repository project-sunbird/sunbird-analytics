#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3
export JAVA_HOME=/usr/java/current

## Job to run daily
cd /mnt/data/analytics/scripts
endDate=$(date --date yesterday "+%Y-%m-%d")

la_config='{"search":{"type":"s3","queries":[{"bucket":"prod-data-store","prefix":"ss/","endDate":"'$endDate'","delta":6}]},"model":"org.ekstep.analytics.model.LearnerActivitySummary","modelParams":{"modelVersion":"1.0","modelId":"LearnerActivitySummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.207:9092","topic":"production.telemetry.derived"}}],"parallelization":8,"appName":"Learner Activity Summarizer","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerActivitySummarizer /mnt/data/analytics/models/batch-models-1.0.jar --config "$la_config" > "logs/$endDate-la-summ.log" 2>&1&