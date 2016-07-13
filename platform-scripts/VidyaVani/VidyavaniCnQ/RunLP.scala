import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.updater._
import org.ekstep.analytics.model._
import org.apache.spark.SparkContext

//val inputFile = "/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/RecoEngine/Data/inputRE.txt";
//val inputFile = "/Users/adarsa/ilimi/github/Learning-Platform-Analytics/platform-scripts/RecoEngine/Data/inputRE.txt";

println("### Setting S3 Keys ###")
CommonUtil.setS3Conf(sc);
implicit val sparkContext: SparkContext = sc
println("### Fetching Data with filter ###")
val queries = Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-02-01"), Option("2016-07-05"))));
val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("S3", None, queries));
val rddAll = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
//val ordinalWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.ordinal.worksheet".equals(e.dimensions.gdata.get.id)).cache();
//val moneyWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.money.worksheet".equals(e.dimensions.gdata.get.id)).cache();
//val numWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.numchart.worksheet".equals(e.dimensions.gdata.get.id)).cache();
//val moreWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.moreless.worksheet".equals(e.dimensions.gdata.get.id)).cache();

//val rdd1 = ordinalWorkSheetRDD.union(moneyWorkSheetRDD)
//val rdd2 = numWorkSheetRDD.union(moreWorkSheetRDD)
//val rddAll = rdd1.union(rdd2)
// rddAll2.saveAsTextFile(inputFile);
// val rddAll = sc.textFile(inputFile).asInstanceof(org.apache.spark.rdd.RDD[org.ekstep.analytics.framework.MeasuredEvent])

println("### Running LCAS ###")
LearnerContentActivitySummary.execute(rddAll,None);
println("### Running LP ###")
LearnerProficiencySummary.execute(rddAll,Option(Map("apiVersion" -> "v2")));
println("### Running RE ###")

println("### Running Learner Snapshot ###")
LearnerActivitySummary.execute(rddAll, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")))
System.exit(0)
