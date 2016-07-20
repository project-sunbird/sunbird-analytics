import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.updater._
import org.ekstep.analytics.model._
import org.apache.spark.SparkContext

println("### Setting S3 Keys ###")
CommonUtil.setS3Conf(sc);
implicit val sparkContext: SparkContext = sc
println("### Fetching Data with filter ###")
val queries = Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-02-01"), Option("2016-07-05"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("S3", None, queries));
val rddAll = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));

println("### Running LCAS ###")
LearnerContentActivitySummary.execute(rddAll,None);
println("### Running LP ###")
LearnerProficiencySummary.execute(rddAll,Option(Map("apiVersion" -> "v2")));
println("### Running RE ###")

println("### Running Learner Snapshot ###")
LearnerActivitySummary.execute(rddAll, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")))
System.exit(0)
