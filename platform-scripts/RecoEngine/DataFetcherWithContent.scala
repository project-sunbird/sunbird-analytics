import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext

println("### Setting S3 Keys ###")
CommonUtil.setS3Conf(sc);
implicit val sparkContext: SparkContext = sc
println("### Fetching Data with filter ###")

val queriesSS = Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-01-01"), Option("2016-01-11"))));
val rddSS = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("S3", None, queriesSS));
val ssData = DataFilter.filter(rddSS, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
val ssRDD = ssData.filter{x=> "numeracy_369".equals(x.dimensions.gdata.get.id)}
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "ss_numeracy_369.log")), ssRDD.map { x => JSONUtils.serialize(x) })

val queriesRAW = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2016-01-01"), Option("2016-01-11"))));
val rddRAW = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queriesRAW)).filter{x=> "numeracy_369".equals(x.gdata.id)};
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "raw_numeracy_369.log")), rddRAW.map { x => JSONUtils.serialize(x) })
System.exit(0)