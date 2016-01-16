#!/usr/bin/env bash

export aws_key="AKIAIUGCZGHRNX2ZO3CQ"
export aws_secret="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"
export SPARK_HOME=/home/ec2-user/spark-1.5.2-bin-hadoop2.3
export JAVA_HOME=/usr/java/current

## Job to run daily
cd /mnt/data/analytics/scripts
endDate=$(date --date yesterday "+%Y-%m-%d")

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"'$endDate'","delta":0}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.AserScreenSummarizer /mnt/data/analytics/models/batch-models-1.0.jar --config "$aser_config" > "logs/$endDate-aser-ss.log" 2>&1&