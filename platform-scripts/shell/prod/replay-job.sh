#!/usr/bin/env bash
export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3
export MODELS_HOME=/mnt/data/analytics/models

cd /mnt/data/analytics/scripts
source model-config.sh
source replay-utils.sh

job_config=$(config $1 '__endDate__')
start_date=$2
end_date=$3

backup $start_date $end_date "sandbox-data-store" "$1" "backup-$1"
if [ $? == 0 ]
 	then
  	echo "Backup completed Successfully..."
  	echo "Running the $1 job replay..."
  	$SPARK_HOME/bin/spark-submit --master local[*] --jars $MODELS_HOME/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor $MODELS_HOME/batch-models-1.0.jar --model "$1" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-$1-replay.log"
else
  	echo "Unable to take backup"
fi

if [ $? == 0 ]
	then
	echo "$1 replay executed successfully"
	delete "sandbox-data-store" "backup-$1"
else
	echo "$1 replay failed"
 	rollback "sandbox-data-store" "$1" "backup-$1"
 	delete "sandbox-data-store" "backup-$1"
fi