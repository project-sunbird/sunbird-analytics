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
import org.ekstep.analytics.framework.FrameworkContext

object ProcessPortalEvents extends optional.Application {

    def main(topic: String, brokerList: String) {

        implicit val fc = new FrameworkContext();
        val queryConfig = """{"type":"local","queries":[{"file":"/mnt/data/analytics/prod.diksha.portal.valid.gz"}]}""";
        implicit val sparkContext = CommonUtil.getSparkContext(10, "ProcessPortalEvents");
        val data = DataFetcher.fetchBatchData[String](JSONUtils.deserialize[Fetcher](queryConfig));
        //val config = Map("topic" -> topic, "brokerList" -> brokerList)
        //OutputDispatcher.dispatch(Dispatcher("kafka", config), data);
        Console.println("Republish to kafka complete!!!", data.count());
    }
    
}