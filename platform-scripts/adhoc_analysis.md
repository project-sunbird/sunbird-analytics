## Adhoc Analysis using Spark Shell

## Setup

```scala
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
CommonUtil.setS3Conf(sc);
```

## ASER Summary Events Mean Computation

**S3 Files**

```java
val queries = Option(Array(Query(Option("ekstep-session-summary"), Option("prod.analytics.screener-"), Option("2015-12-20"), Option("2015-12-27"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("S3", None, queries));
val aserRDD = rdd.filter(e => "org.ekstep.aser.lite".equals(e.dimensions.gdata.get.id.get)).cache();
val timeSpent = aserRDD.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("timeSpent", 0d).asInstanceOf[Double]).cache();
timeSpent.mean();
```

**Local Files**

```java
val queries = Option(Array(JSONUtils.deserialize[Query]("{\"file\":\"/mnt/data/analytics/akshara_session_summary.log\"}")))
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("local", None, queries));
val timeSpent = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("timeSpent", 0d).asInstanceOf[Double]).cache();
timeSpent.mean();
```

## ASER Screen Summary Events Mean Computation

**Local Files**

```java
val queries = Option(Array(JSONUtils.deserialize[Query]("{\"file\":\"/mnt/data/analytics/aser-screen-summary.log\"}")))
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("local", None, queries));
val akp = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("activationKeyPage", 0d).asInstanceOf[Double]).cache();
val scp = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("surveyCodePage", 0d).asInstanceOf[Double]).cache();
val cr1 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("childReg1", 0d).asInstanceOf[Double]).cache();
val cr2 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("childReg2", 0d).asInstanceOf[Double]).cache();
val cr3 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("childReg3", 0d).asInstanceOf[Double]).cache();
val al = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessLanguage", 0d).asInstanceOf[Double]).cache();
val ll = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("languageLevel", 0d).asInstanceOf[Double]).cache();
val snq1 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("selectNumeracyQ1", 0d).asInstanceOf[Double]).cache();
val an1 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessNumeracyQ1", 0d).asInstanceOf[Double]).cache();
val sn2 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("selectNumeracyQ2", 0d).asInstanceOf[Double]).cache();
val an2 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessNumeracyQ2", 0d).asInstanceOf[Double]).cache();
val an3 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessNumeracyQ3", 0d).asInstanceOf[Double]).cache();
val sc = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("scorecard", 0d).asInstanceOf[Double]).cache();
val su = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("summary", 0d).asInstanceOf[Double]).cache();
print(akp.mean,scp.mean,cr1.mean,cr2.mean,cr3.mean,al.mean,ll.mean,snq1.mean,an1.mean,sn2.mean,an2.mean,an3.mean,sc.mean,su.mean);

akp.mean+scp.mean+cr1.mean+cr2.mean+cr3.mean+al.mean+ll.mean+snq1.mean+an1.mean+sn2.mean+an2.mean+an3.mean+sc.mean+su.mean
294
```

## Events to Local File

```java
val queries = Option(Array(Query(Option("ekstep-telemetry"), Option("prod.telemetry.unique-"), Option("2015-10-27"), Option("2016-01-04"))));
val rdd = DataFetcher.fetchBatchData[Map[String,AnyRef]](sc, Fetcher("S3", None, queries));
val aserRDD = DataFilter.filter(rdd, Filter("userId", "EQ", Option("409fe811-ef92-4ec5-b5f6-aceb787fc9ec"))).map(e => JSONUtils.serialize(e));
val aserRDD = rdd.filter(m => "409fe811-ef92-4ec5-b5f6-aceb787fc9ec".equals(m.getOrElse("uid","").asInstanceOf[String])).map(e => JSONUtils.serialize(e));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/mnt/data/analytics/409fe811-ef92-4ec5-b5f6-aceb787fc9ec.log")), aserRDD);
```

## Aser Session Summary Events to local file

**Config for Local Files**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/ss-aser-1.log"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-07","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/ss-aser-2.log"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'
```

**Config for Kafka**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-08","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'
```

**Run Job**

```sh
nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.GenericSessionSummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$aser_config" >> "batch-aser.log" &
```

## Akshara Session Summary Events to local file

```sh
akshara_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-07","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"numeracy_369"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_369","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/ss-akshara.log"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

akshara_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-08","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"numeracy_369"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_369","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.GenericSessionSummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$akshara_config" >> "batch-akshara.log" &
```


## Aser Lite Screen Summary Events to local file

**Config for Local File**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/aser-screen-summary.log"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-07","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/aser-screen-summary.log"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'
```

**Config for Kafka**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-08","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'
```

```sh
nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.AserScreenSummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$aser_config" >> "batch-aser-screen.log" &
```

## Learner Activity Summary Events from and to local file

```sh
la_config='{"search":{"type":"local","queries":[{"file":"/mnt/data/analytics/ss*.log"}]},"filters":[],"model":"org.ekstep.analytics.model.LearnerActivitySummary","modelParams":{"modelVersion":"1.0"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/la-events.log"}}],"parallelization":8,"appName":"Learner Activity Summarizer","deviceMapping":false}'

nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerActivitySummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$la_config" >> "batch-la.log" &
```

## Aser sessions with more than one attempt

```java
val queries = Option(Array(Query(Option("ekstep-session-summary"), Option("prod.analytics.screener-"), Option("2015-12-28"), Option("2015-12-29"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("S3", None, queries));
val aserRDD = rdd.filter(e => "org.ekstep.aser.lite".equals(e.dimensions.gdata.get.id)).cache();
val noOfAttempts = aserRDD.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("noOfAttempts", 0).asInstanceOf[Int]).cache();
timeSpent.mean();
```

## Output events to file

```java
val rdd = loadFile("/Users/Santhosh/Downloads/prod.telemetry.unique-2016-01-02-06-32.json");
val rdd2 = DataFilter.filter(rdd, Array(Filter("eid","IN",Option(List("OE_START","OE_END","OE_ASSESS","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"))),
                Filter("gdata.id","EQ",Option("org.ekstep.aser.lite"))));
val rdd3 = rdd2.map { x => JSONUtils.serialize(x) };
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/Users/Santhosh/ekStep/github-repos/Learning-Platform-Analytics/platform-modules/batch-models/src/test/resources/test_data1.log")), rdd3)
```