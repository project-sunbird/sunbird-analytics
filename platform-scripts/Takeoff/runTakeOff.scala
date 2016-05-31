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
val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2016-01-01"), Option("2016-04-30"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
 

// session-summary data to local file
val queries = Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-01-01"), Option("2016-04-30"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("S3", None, queries));
val sessSummaries = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
val takeoffrdd = sessSummaries.filter(e => "org.ekstep.delta".equals(e.dimensions.gdata.get.id)).cache();


// write to file
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/SSDeltaDump2016tillApril.txt"))), rddAsString);



// write raw data to local file
val queries = Option(Array(Query(Option("sandbox-data-store"), Option("raw/"), Option("2016-04-27"), Option("2016-05-07"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/DeltaDumpLadakh.txt"))), rddAsString);

 // session-summary data to local file
val queries = Option(Array(Query(Option("sandbox-data-store"), Option("ss/"), Option("2016-04-27"), Option("2016-05-07"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("S3", None, queries));
val sessSummaries = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
val takeoffrdd = sessSummaries.filter(e => "org.ekstep.delta".equals(e.dimensions.gdata.get.id)).cache();
// write to file
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/SSDeltaDumpLadakh.txt"))), rddAsString);


System.exit(0)



System.exit(0)



// take-off in Lakdakh


