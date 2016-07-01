import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.updater._
import org.ekstep.analytics.model._
import org.apache.spark.SparkContext


val outputPath = "/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/Takeoff";
//val inputFile = "/Users/adarsa/ilimi/github/Learning-Platform-Analytics/platform-scripts/RecoEngine/Data/inputRE.txt";

println("### Setting S3 Keys ###")
CommonUtil.setS3Conf(sc);
implicit val sparkContext: SparkContext = sc
println("### Fetching Data with filter ###")



// write raw data to local file
val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2015-01-01"), Option("2015-12-31"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/DeltaDump2015.txt"))), rddAsString);


val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2016-01-01"), Option("2016-04-31"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/DeltaDump2016Jan2April.txt"))), rddAsString);

val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2016-05-01"), Option("2016-06-22"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/DeltaDump2016May2June22.txt"))), rddAsString);


// write raw data to local file
val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2015-0-01"), Option("2016-04-30"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
// write to file
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/DeltaDump2016tillApril.txt"))), rddAsString);

 

// session-summary data to local file
val queries = Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-01-01"), Option("2016-04-30"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("S3", None, queries));
val sessSummaries = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
val takeoffrdd = sessSummaries.filter(e => "org.ekstep.delta".equals(e.dimensions.gdata.get.id)).cache();


// write to file
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/SSDeltaDump2016tillApril.txt"))), rddAsString);



// write raw data to local file
val queries = Option(Array(Query(Option("sandbox-data-store"), Option("raw/"), Option("2016-04-27"), Option("2016-05-31"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/DeltaDumpLadakhMay.txt"))), rddAsString);

 // session-summary data to local file
val queries = Option(Array(Query(Option("sandbox-data-store"), Option("ss/"), Option("2016-04-27"), Option("2016-05-30"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("S3", None, queries));
val sessSummaries = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
val takeOffrdd = sessSummaries.filter(e => "org.ekstep.delta".equals(e.dimensions.gdata.get.id)).cache();
// write to file
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/SSDeltaDumpLadakh.txt"))), rddAsString);


System.exit(0)



System.exit(0)



// take-off in Lakdakh


