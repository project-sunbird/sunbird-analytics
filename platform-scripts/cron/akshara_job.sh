#!/usr/bin/env bash

## Job to run daily
cd /mnt/data/analytics/scripts
endDate=$(date --date yesterday "+%Y-%m-%d")

akshara_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"'$endDate'","delta":0}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET"]},{"name":"gameId","operator":"EQ","value":"numeracy_369"}],"model":"org.ekstep.analytics.model.GenericScreenerSummary","modelParams":{"contentId":"numeracy_369","modelVersion":"1.1","modelId":"GenericContentSummary"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Activity Screener Summary"}'

nohup spark-submit --master local[*] --conf 'spark.driver.extraJavaOptions=-Denv=prod' --class org.ekstep.ilimi.analytics.framework.JobDriver /home/ec2-user/models/analytics-framework-0.2.jar --t "batch" --config "$akshara_config" > "logs/$endDate-akshara.log" 2>&1&