#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3
export MODELS_HOME=/mnt/data/analytics/models

## Job to run daily
cd /mnt/data/analytics/scripts
source model-config.sh
today=$(date "+%Y-%m-%d")

job_config=$(config $1)

echo "Starting the job - $1" > "logs/$today-job-execution.log"
nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars $MODELS_HOME/analytics-framework-0.5.jar --class org.ekstep.analytics.job.JobExecutor $MODELS_HOME/batch-models-1.0.jar --model "$1" --config "$job_config" > "logs/$today-job-execution.log" 2>&1
echo "Job execution completed - $1" > "logs/$today-job-execution.log"