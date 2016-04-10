#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

cd /mnt/data/analytics/scripts

start_date=$1
end_date=$2
job_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":0}]},"filters":[{"name":"ver","operator":"EQ","value":"1.0"}],"model":"org.ekstep.analytics.model.LearnerSessionSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.analytics.screener"}}],"parallelization":8,"appName":"Learner Session Summarizer","deviceMapping":true}'
echo "Backing up the proficiency summary records to s3://sandbox-data-store/backup-ss"
./replay-backup.sh $start_date $end_date "sandbox-data-store" "ss" "backup-ss"
if [ $? == 0 ]
 	then
  	echo "Backup completed Successfully..."
  	echo "Running the Proficiency Updater Replay..."
  	$SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "ss" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-ss-replay.log"
else
  	echo "Unable to take backup"
fi

if [ $? == 0 ]
	then
  		echo "Proficiency Updater Replay Executed Successfully..."
  		echo "Deleting the back-up files s3://sandbox-data-store/backup-ss "
  		./replay-delete.sh "sandbox-data-store" "backup-ss"
else
 	echo "Unable to take backup"
fi