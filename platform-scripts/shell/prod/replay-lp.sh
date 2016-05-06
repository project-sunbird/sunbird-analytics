#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

cd /mnt/data/analytics/scripts

start_date=$1
end_date=$2
job_config='{"search":{"type":"s3","queries":[{"bucket":"prod-data-store","prefix":"ss/","endDate":"__endDate__","delta":0}]},"model":"org.ekstep.analytics.model.ProficiencyUpdater","modelParams":{"apiVersion":"v2","alpha":1.0,"beta":1.0},"output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"10.10.1.207:9092","topic":"production.telemetry.derived"}}],"parallelization":10,"appName":"Proficiency Updater","deviceMapping":false}'

echo "Backing up the proficiency summary records to s3://prod-data-store/backup-lp"
./replay-backup.sh $start_date $end_date "prod-data-store" "lp" "backup-lp"
if [ $? == 0 ]
 	then
  	echo "Backup completed Successfully..."
  	echo "Running the Proficiency Updater Replay..."
  	$SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "lp" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-lp-replay.log"
else
  	echo "Unable to take backup"
fi

if [ $? == 0 ]
	then
  		echo "Proficiency Updater Replay Executed Successfully..."
  		echo "Deleting the back-up files s3://prod-data-store/backup-lp "
  		./replay-delete.sh "prod-data-store" "backup-lp"
else
 	echo "Copy back the Proficiency Updater files to source directory '/lp' from backup directory '/backup-lp'"
 	./replay-copy-back.sh "prod-data-store" "lp" "backup-lp"
 	echo "Deleting the back-up files s3://prod-data-store/backup-lp "
    ./replay-delete.sh "prod-data-store" "backup-lp"	
fi