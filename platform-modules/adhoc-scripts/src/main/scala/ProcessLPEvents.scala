import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JSONUtils

object ProcessLPEvents extends optional.Application {

    def main(bucket: String, prefix: String, topic: String, brokerList: String) {

        val queryConfig = """{"type":"local","queries":[{"file":"/mnt/data/analytics/lp_events/"""+ prefix +""""}]}""";
        implicit val sparkContext = CommonUtil.getSparkContext(10, "InvalidLPEvents");
        val data = DataFetcher.fetchBatchData[Map[String, AnyRef]](JSONUtils.deserialize[Fetcher](queryConfig));
        val lpevents = data.filter(f => !f.contains("@timestamp")).filter(f => f.contains("ets")).map(f => {
	        Map("@timestamp" -> CommonUtil.df5.print(f.get("ets").get.asInstanceOf[Double].toLong)) ++ f
        }).map(f => JSONUtils.serialize(f));
        val config = Map("topic" -> topic, "brokerList" -> brokerList)
        OutputDispatcher.dispatch(Dispatcher("kafka", config), lpevents);
        Console.println("Republish to kafka complete!!!");
    }
    
}