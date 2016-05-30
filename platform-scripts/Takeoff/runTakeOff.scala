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

//val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2015-08-01"), Option("2015-12-30"))));
val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2016-01-01"), Option("2016-04-30"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val takeOffrdd = rdd.filter(e => "org.ekstep.delta".equals(e.gdata.id))
val rddAsString = takeOffrdd.map(JSONUtils.serialize(_));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> (outputPath+"/DeltaDump2016tillApril.txt"))), rddAsString);
System.exit(0)
