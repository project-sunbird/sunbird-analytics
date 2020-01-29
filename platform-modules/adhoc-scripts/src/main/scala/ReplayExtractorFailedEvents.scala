import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.FrameworkContext
import org.apache.spark.SparkContext

object ReplayExtractorFailedEvents extends optional.Application {

  case class BatchEvent(flags: Option[Map[String, AnyRef]]);

  def main(from: String, to: String, brokerList: String, topic: String): Unit = {
    implicit val sparkContext = CommonUtil.getSparkContext(16, "ReplayExtractorFailedEvents");
    implicit val fc = new FrameworkContext();
    try {
      val dates = CommonUtil.getDatesBetween(from, Option(to)).toSeq;
      Console.println("Dates", dates);
      val data = for (date <- dates) yield {
        execute(date, brokerList, topic);
      }
    } finally {
      sparkContext.stop();
    }
  }

  def execute(date: String, brokerList: String, topic: String)(implicit sc: SparkContext, fc: FrameworkContext) {
    
    Console.println("Replaying the missing events for date", date);
    val failedConfig = s"""{"type":"azure","queries":[{"bucket": "telemetry-data-store", "prefix": "extractor-failed/", "endDate": "$date","delta": 0}]}""";
    val failedData = DataFetcher.fetchBatchData[String](JSONUtils.deserialize[Fetcher](failedConfig));
    val missingData = failedData.map(f => (JSONUtils.deserialize[BatchEvent](f), f)).filter(f => f._1.flags.isEmpty).map(f => f._2).cache();
    Console.println("Total Missing Events:", missingData.count());
    OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> brokerList, "topic" -> topic)), missingData);
    //OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "missing-events.log")), missingData);
  }
}