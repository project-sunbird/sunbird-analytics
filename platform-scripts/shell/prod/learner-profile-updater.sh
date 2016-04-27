#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3
export JAVA_HOME=/usr/java/current

## Job to run daily
cd /mnt/data/analytics/scripts
endDate=$(date --date yesterday "+%Y-%m-%d")

lpu_config='{"search":{"type":"s3","queries":[{"bucket":"prod-data-store","prefix":"raw/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.updater.LearnerProfileUpdater","modelParams":{"modelVersion":"1.0","modelId":"LearnerProfileUpdater"},"output":[{"to":"console","params":{"printEvent": false}}],"parallelization":8,"appName":"Learner Profile Updater","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerProfileUpdaterJob /mnt/data/analytics/models/batch-models-1.0.jar --config "$lpu_config" > "logs/$endDate-lp-updater.log" 2>&1&