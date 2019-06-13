import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}

case class Params(msgid: String)
case class BatchEvent(mid: String, pid: String, params: Option[Params], events: Array[V3Event])
case class BatchEventLite(mid: String, pid: String, params: Option[Params])
case class V3EventLite(mid: String, eid: String);

object DedupAnalyzer extends optional.Application {

  def main(config: String, isTest: Boolean) {

    val execTime = CommonUtil.time({
      implicit val sparkContext = CommonUtil.getSparkContext(10, "DedupAnalyzer")

      //val queries = Option(Array(Query(Option("telemetry-data-store"), Option("ingest/"), None, Option("2019-06-05"), Option(0))));
      //val rdd = DataFetcher.fetchBatchData[BatchEvent](Fetcher("azure", None, queries)).filter(f=> "prod.diksha.app".equals(f.pid)).cache();
      analyzeDuplicatedByEID();
      //analyzeDedupOptimizations();

      sparkContext.stop
    })
    Console.println("Time taken to execute:", execTime._1);
  }

  private def analyzeDedupOptimizations()(implicit sc: SparkContext): Unit = {

    val avgKeySize = 155;
    val queryConfig = """{"type":"local","queries":[{"file":"/Users/santhosh/EkStep/telemetry/raw/2019-06-07*.json.gz"}]}""";
    val events = DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig)).cache();


    val eventsCount = events.count();
    val distinctCount = events.map(f => f.mid).distinct().count();
    val duplicateKeys = eventsCount - distinctCount;

    val requiredRedisSize = (distinctCount * avgKeySize)/(1000 * 1000);

    val nonMobileEvents = events.filter(f => !"prod.diksha.app".equals(f.context.pdata.getOrElse(V3PData("")).  id));

    val distinctNonMobileEventsCount = nonMobileEvents.map(f => f.mid).distinct().count();
    val distinctNMEExcludingIrrelevantEvents = nonMobileEvents.filter(f => !Array("LOG","ERROR","SEARCH").contains(f.eid)).map(f => f.mid).distinct().count();

    val distinctKeysExcludingIrrelevantEvents = events.filter(f => !Array("LOG","ERROR","SEARCH").contains(f.eid)).map(f => f.mid).distinct().count();

    events.unpersist(true);

    val queryConfig2 = """{"type":"local","queries":[{"file":"/Users/santhosh/EkStep/telemetry/ingest/2019-06-07*.json.gz"}]}""";
    val batchEvents = DataFetcher.fetchBatchData[BatchEventLite](JSONUtils.deserialize[Fetcher](queryConfig2));
    val mobileAppKeysCount = batchEvents.filter(f=> "prod.diksha.app".equals(f.pid)).map(f => f.params.get.msgid).distinct().count();

    val op1KeysCount = (distinctNonMobileEventsCount + mobileAppKeysCount);
    val op1RedisSize = (op1KeysCount * avgKeySize) / (1000 * 1000);

    val op2KeysCount = (distinctNMEExcludingIrrelevantEvents + mobileAppKeysCount);
    val op2RedisSize = (op2KeysCount * avgKeySize) / (1000 * 1000)

    val op3KeysCount = distinctKeysExcludingIrrelevantEvents/20;
    val op3RedisSize = (op3KeysCount * avgKeySize) / (1000 * 1000)

    Console.println("##### Current Dedup state #### ")
    Console.println("Total Events - ", eventsCount)
    Console.println("Duplicates - ", duplicateKeys)
    Console.println("Duplicates % - ", (duplicateKeys * 100)/eventsCount);

    Console.println("Keys in Redis - ", distinctCount)
    Console.println("Redis cache size required in MB - ", requiredRedisSize);
    Console.println("");

    Console.println("##### Optimization 1 - Dedup mobile app events by batch #### ")
    Console.println("Keys in Redis - ", op1KeysCount)
    Console.println("Redis cache size required in MB - ", op1RedisSize)

    Console.println("##### Optimization 2 - Optimization 1 + Skip LOG, ERROR, SEARCH events #### ")
    Console.println("Keys in Redis - ", op2KeysCount)
    Console.println("Redis cache size required in MB - ", op2RedisSize)

    Console.println("##### Optimization 3 - Dedup all events by batch size of 20, excluding LOG, ERROR & SEARCH events #### ")
    Console.println("Keys in Redis - ", op3KeysCount)
    Console.println("Redis cache size required in MB - ", op3RedisSize)


  }

  private def analyzeDuplicatedByEID()(implicit sc: SparkContext): Unit = {
    val queryConfig = """{"type":"local","queries":[{"file":"/Users/santhosh/EkStep/telemetry/raw/2019-06-07*.json.gz"}]}""";
    val events = DataFetcher.fetchBatchData[V3EventLite](JSONUtils.deserialize[Fetcher](queryConfig));
    events.groupBy(f => f).mapValues(f => f.size).filter(f => f._2 > 1).groupBy(f => f._1.eid).mapValues(f => {
      f.map(f => f._2).sum + f.size
    }).collect().foreach(println(_));
  }

  private def analyzePortalEvents()(implicit sc: SparkContext): Unit = {
    val queryConfig = """{"type":"local","queries":[{"file":"/Users/santhosh/EkStep/telemetry/ingest/2019-06-07*.json.gz"}]}""";
    val rdd = DataFetcher.fetchBatchData[BatchEvent](JSONUtils.deserialize[Fetcher](queryConfig)).filter(f=> "prod.diksha.portal".equals(f.pid)).cache();
    Console.println("Dedup raw events...", rdd.count)
    dedupEvents(rdd);
  }


  private def analyzeMobileAppEvents()(implicit sc: SparkContext) {
    val queryConfig = """{"type":"local","queries":[{"file":"/Users/santhosh/EkStep/telemetry/ingest/2019-06-07*.json.gz"}]}""";
    val rdd = DataFetcher.fetchBatchData[BatchEvent](JSONUtils.deserialize[Fetcher](queryConfig)).filter(f=> "prod.diksha.app".equals(f.pid)).cache();
    Console.println("Dedup raw events...", rdd.count)
    dedupEvents(rdd);

    val dedupRdd = dedupBatchEvents(rdd);
    Console.println("Dedup batch events...", dedupRdd.count())
    dedupEvents(dedupRdd);
  }

  private def dedupEvents(rdd: RDD[BatchEvent]): Unit = {
    val events = rdd.map(f => f.events).filter(f => f != null).flatMap(f => f.map(f => f)).map(f => f.mid);
    val count = events.count;
    val distinctCount = events.distinct().count
    Console.println("Event Count:", count, "Distinct Count:", distinctCount, "Duplicates:", (count - distinctCount))
  }

  private def dedupBatchEvents(rdd: RDD[BatchEvent]): RDD[BatchEvent] = {
    rdd.groupBy(f => f.params.get.msgid).mapValues(f => f.head).map(f => f._2)
  }

}

object TestDedupAnalyzer {

  def main(args: Array[String]): Unit = {
    DedupAnalyzer.main("", true);
  }

}