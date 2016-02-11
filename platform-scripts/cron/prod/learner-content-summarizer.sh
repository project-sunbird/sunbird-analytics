#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3
export JAVA_HOME=/usr/java/current

## Job to run daily
cd /mnt/data/analytics/scripts
endDate=$(date "+%Y-%m-%d")

lc_config='{"search":{"type":"s3","queries":[{"bucket":"ekstep-session-summary","prefix":"prod.analytics.screener-","endDate":"'$endDate'","delta":6}]},"filters":[{"name":"eventId","operator":"EQ","value":"ME_SESSION_SUMMARY"}],"model":"org.ekstep.analytics.model.LearnerContentActivitySummary","output":[{"to":"console","params":{"printEvent": false}}],"parallelization":8,"appName":"Learner Content Activity Summarizer","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerContentActivityUpdater /mnt/data/analytics/models/batch-models-1.0.jar --config "$lc_config" > "logs/$endDate-lc-summ.log" 2>&1&