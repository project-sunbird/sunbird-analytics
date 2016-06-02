#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

## Job to run daily
cd /mnt/data/analytics/scripts
source model-config.sh
today=$(date "+%Y-%m-%d")

job_config=$(config '$1')

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.JobExecutor models/batch-models-1.0.jar --model "$1" --config "$job_config" > "logs/$today-job-execution.log" 2>&1&