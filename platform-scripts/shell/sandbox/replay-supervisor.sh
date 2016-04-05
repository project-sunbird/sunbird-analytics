#!/usr/bin/env bash

export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3

## Job to run daily
cd /mnt/data/analytics/scripts
endDate=$(date --date yesterday "+%Y-%m-%d")

model_code="lp"
start_date="2016-01-01"
end_date="2016-02-01"
job_config='{"search":{"type":"s3","queries":[{"bucket":"prod-data-store","prefix":"ss/","endDate":"__endDate__","delta":0}]},"filters":[{"name":"eid","operator":"EQ","value":"ME_SESSION_SUMMARY"}],"model":"org.ekstep.analytics.model.ProficiencyUpdater","modelParams":{"alpha":1.0,"beta":1.0},"output":[{"to":"console","params":{"printEvent":false}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"replay"}}],"parallelization":10,"appName":"TestReplaySupervisor","deviceMapping":false}'

./replay-backup $start_date $end_date $model_code
if [ $? == 0 ]
 then
  echo "Backup Done Successfully..."
  nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.ReplaySupervisor /mnt/data/analytics/models/batch-models-1.0.jar --model "$model_code" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-lp.log" 2>&1&
 else
  echo "Unable to take backup"
fi

if [ $? == 0 ]
 then
  echo "Replay Supervisor Executed Successfully..."
  echo "Deleting the back-up files"
  ./replay-delete $start_date $end_date $model_code
 else
  echo "Unable to take backup"
fi

