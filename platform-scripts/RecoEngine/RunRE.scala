import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.updater._
import org.ekstep.analytics.model._
println("### Setting S3 Keys ###")
CommonUtil.setS3Conf(sc);
println("### Fetching Data with filter ###")
val queries = Option(Array(Query(Option("ekstep-session-summary"), Option("prod.analytics.screener-"), Option("2016-02-21"), Option("2016-02-23"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("S3", None, queries));
val sessSummaries = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
val ordinalWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.ordinal.worksheet".equals(e.dimensions.gdata.get.id)).cache();
val moneyWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.money.worksheet".equals(e.dimensions.gdata.get.id)).cache();
val numWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.numchart.worksheet".equals(e.dimensions.gdata.get.id)).cache();
val moreWorkSheetRDD = sessSummaries.filter(e => "org.ekstep.moreless.worksheet".equals(e.dimensions.gdata.get.id)).cache();

val rdd1 = ordinalWorkSheetRDD.union(moneyWorkSheetRDD)
val rdd2 = numWorkSheetRDD.union(moreWorkSheetRDD)
val rddAll = rdd1.union(rdd2)
println("### Running LCAS ###")
LearnerContentActivitySummary.execute(sc,rddAll,None);
println("### Running LP ###")
LearnerProficiencySummary.execute(sc,rddAll,Option(Map("apiVersion" -> "v2")));
println("### Running RE ###")
RecommendationEngine.execute(sc, rddAll, None);
println("### Running Learner Snapshot ###")
LearnerActivitySummary.execute(sc, rddAll, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")))
System.exit(0)
