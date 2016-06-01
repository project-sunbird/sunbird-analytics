#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

cd /mnt/data/analytics/scripts

start_date=$1
end_date=$2
job_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.GenieLaunchSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Genie Launch Summarizer","deviceMapping":true}'
echo "Backing up the Genie Launch Summary records to s3://sandbox-data-store/backup-gls"
./replay-backup.sh $start_date $end_date "sandbox-data-store" "gls" "backup-gls"
if [ $? == 0 ]
 	then
  	echo "Backup completed Successfully..."
  	echo "Running the Genie Launch Summary Replay..."
  	$SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "gls" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-gls-replay.log"
else
  	echo "Unable to take backup"
fi

if [ $? == 0 ]
	then
  		echo "Genie Launch Summary Replay Executed Successfully..."
  		echo "Deleting the back-up files s3://sandbox-data-store/backup-gls"
  		./replay-delete.sh "sandbox-data-store" "backup-gls"
else
 	echo "Copy back the Genie Launch Summarizer files to source directory '/gls' from backup directory '/backup-gls'"
 	./replay-copy-back.sh "sandbox-data-store" "gls" "backup-gls"
 	echo "Deleting the back-up files s3://sandbox-data-store/backup-gls"
 	./replay-delete.sh "sandbox-data-store" "backup-gls"
fi