#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

cd /mnt/data/analytics/scripts

start_date=$1
end_date=$2
job_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":6}]},"model":"org.ekstep.analytics.updater.LearnerProfileUpdater","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"Learner Profile Updater","deviceMapping":false}'

echo "Running the Learner Profile Updater Replay..."
$SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "lpu" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-lpu-replay.log"

if [ $? == 0 ] 
	then
  		echo "Learner Profile Updater Replay Executed Successfully..."
else
 	echo "Unable to run Learner Profile Updater replay"
fi