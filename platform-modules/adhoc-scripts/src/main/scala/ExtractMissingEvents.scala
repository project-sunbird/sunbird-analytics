import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils

object ExtractMissingEvents {

  val processingDate = "2019-09-02"
  val ingestEventsData = "/home/spark/ingest_backup_data"
  val rawEventsData = "/home/spark/raw_events_data"
  val outputDir = s"/mount/data/analytics/replay-data/missing_events/$processingDate"

  implicit val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
  implicit val sparkContext: SparkContext = sparkSession.sparkContext
  implicit val fc: FrameworkContext = new FrameworkContext();

  def extractMissingDenormEvents() = {
    val denormEventsConfig =
      s"""{"search":{"type":"azure","queries":[{"bucket":"telemetry-data-store","prefix":"unique/",
         |"endDate":"$processingDate","delta":0}]},"model":"org.ekstep.analytics.job.ExtractBatchEvents","modelParams":{},
         |"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"ExtractBatchFailedEvents",
         |"deviceMapping":false}""".stripMargin
    val denormEvents = DataFetcher.fetchBatchData[Map[String, AnyRef]](JSONUtils.deserialize[JobConfig](denormEventsConfig).search)
    println("denorm_events = " + denormEvents.count)
    val pairDenormBCRdd = sparkContext.broadcast(denormEvents.map { x => x("mid").asInstanceOf[String] }.collect.toSet)


    val uniqueEventsConfig =
      s"""{"search":{"type":"azure","queries":[{"bucket":"telemetry-data-store","prefix":"unique/",
         |"endDate":"$processingDate","delta":0}]},"model":"org.ekstep.analytics.job.ExtractBatchEvents","modelParams":{},
         |"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"ExtractBatchFailedEvents",
         |"deviceMapping":false}""".stripMargin
    val uniqueEvents = DataFetcher.fetchBatchData[Map[String, AnyRef]](JSONUtils.deserialize[JobConfig](uniqueEventsConfig).search).filter {
      event =>
        val eid = event.getOrElse("eid", "").asInstanceOf[String]
        !(eid.equalsIgnoreCase("LOG") || eid.equalsIgnoreCase("ERROR"))
    }
    println("unique_events = " + uniqueEvents.count)
    val pairUniqueRdd = uniqueEvents.map{ x => (x("mid").asInstanceOf[String], x) }
    val filteredRdd = pairUniqueRdd.filter { x => !pairDenormBCRdd.value.contains(x._1) }
    println("missing_events = " + filteredRdd.count)
    filteredRdd.map{x => JSONUtils.serialize(x._2)}.coalesce(10).saveAsTextFile(outputDir)
  }

  def extractBatchMissingEvents() = {
    val batchEventsConfig = s"""{"type":"local","queries":[{"file":"$ingestEventsData/$processingDate*.json.gz"}]}"""
    val batchEvents = DataFetcher.fetchBatchData[Map[String, AnyRef]](JSONUtils.deserialize[Fetcher](batchEventsConfig))
    val explodedBatchEvents = batchEvents.flatMap(batchEvent => {
      val eventsArray = batchEvent("events").asInstanceOf[List[Map[String, AnyRef]]]
      eventsArray.map { event =>
        val modifiedMap = event ++ Map("syncts" -> batchEvent("syncts").asInstanceOf[Long])
        modifiedMap
      }
    })
    println("exploded_events = " + explodedBatchEvents.count)
    val explodedEventsPairRDD = explodedBatchEvents.map{ x => (x("mid").asInstanceOf[String], x) }

    val rawEventsConfig = s"""{"type":"local","queries":[{"file":"$rawEventsData/$processingDate*.json.gz"}]}"""
    val rawEvents = DataFetcher.fetchBatchData[Map[String, AnyRef]](JSONUtils.deserialize[Fetcher](rawEventsConfig))
    println("raw_events = " + rawEvents.count)
    val rawEventsPairRDD = sparkContext.broadcast(rawEvents.map { x => x("mid").asInstanceOf[String] }.collect.toSet)
    val missingEventsRDD = explodedEventsPairRDD.filter { x => !rawEventsPairRDD.value.contains(x._1) }
    println("missing_events = " + missingEventsRDD.count)
    missingEventsRDD.map{x => JSONUtils.serialize(x._2)}.coalesce(5).saveAsTextFile(outputDir)

  }

}