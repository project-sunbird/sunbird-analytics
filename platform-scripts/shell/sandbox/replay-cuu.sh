#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

cd /mnt/data/analytics/scripts

start_date=$1
end_date=$2
job_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"cus/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.updater.ContentUsageUpdater","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Content Usage Updater","deviceMapping":false}'

echo "Running the Content Usage Updater Replay..."
$SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "cuu" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-cuu-replay.log"

if [ $? == 0 ] 
	then
  		echo "Content Usage Updater Replay Executed Successfully..."
else
 	echo "Unable to run Content Usage Updater replay"
fi