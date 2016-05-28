#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

cd /mnt/data/analytics/scripts

start_date=$1
end_date=$2
job_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"gls/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.DeviceUsageSummary","output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":10,"appName":"DeviceUsageSummary","deviceMapping":false}'

echo "Backing up the device usage summary records to s3://sandbox-data-store/backup-dus"
./replay-backup.sh $start_date $end_date "sandbox-data-store" "dus" "backup-dus"
if [ $? == 0 ]
 	then
  	echo "Backup completed Successfully..."
  	echo "Running the device Usage Summary Replay..."
  	$SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "dus" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-dus-replay.log"
else
  	echo "Unable to take backup"
fi

if [ $? == 0 ]
	then
  		echo "Device Usage Summary Replay Executed Successfully..."
  		echo "Deleting the back-up files s3://sandbox-data-store/backup-dus "
  		./replay-delete.sh "sandbox-data-store" "backup-dus"
else
 	echo "Copy back the Device Usage Summary files to source directory '/dus' from backup directory '/backup-dus'"
 	./replay-copy-back.sh "sandbox-data-store" "dus" "backup-dus"
 	echo "Deleting the back-up files s3://sandbox-data-store/backup-dus "
	./replay-delete.sh "sandbox-data-store" "backup-dus"
fi