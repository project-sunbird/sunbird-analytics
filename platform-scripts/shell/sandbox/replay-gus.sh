#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

cd /mnt/data/analytics/scripts

start_date=$1
end_date=$2
job_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-data-store","prefix":"raw/","endDate":"__endDate__","delta":0}]},"filters":[{"name":"ver","operator":"EQ","value":"1.0"}],"model":"org.ekstep.analytics.model.GenieUsageSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.telemetry.derived"}}],"parallelization":8,"appName":"Genie Usage Summarizer","deviceMapping":true}'
echo "Backing up the Genie Usage Summary records to s3://sandbox-data-store/backup-gus"
./replay-backup.sh $start_date $end_date "sandbox-data-store" "gus" "backup-gus"
if [ $? == 0 ]
 	then
  	echo "Backup completed Successfully..."
  	echo "Running the Genie Usage Summary Replay..."
  	$SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "gus" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-gus-replay.log"
else
  	echo "Unable to take backup"
fi

if [ $? == 0 ]
	then
  		echo "Genie Usage Summary Replay Executed Successfully..."
  		echo "Deleting the back-up files s3://sandbox-data-store/backup-gus"
  		./replay-delete.sh "sandbox-data-store" "backup-gus"
else
 	echo "Copy back the Genie Usage Summarizer files to source directory '/gus' from backup directory '/backup-gus'"
 	./replay-copy-back.sh "sandbox-data-store" "gus" "backup-gus"
 	echo "Deleting the back-up files s3://sandbox-data-store/backup-gus"
 	./replay-delete.sh "sandbox-data-store" "backup-gus"
fi