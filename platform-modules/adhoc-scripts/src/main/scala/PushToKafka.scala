import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.FrameworkContext
import scala.reflect.ManifestFactory.classType

object PushToKafka {

  def main(args: Array[String]): Unit = {
    val queryConfig = """{"type":"local","queries":[{"file":"/Users/Santhosh/EkStep/telemetry_denorm/2019-11-01*"}]}""";
    implicit val sparkContext = CommonUtil.getSparkContext(10, "PushToKafka");
    implicit val fc = new FrameworkContext();
    val data = DataFetcher.fetchBatchData[String](JSONUtils.deserialize[Fetcher](queryConfig));
    val config = Map("topic" -> "druid-rollup", "brokerList" -> "localhost:9092")
    OutputDispatcher.dispatch(Dispatcher("kafka", config), data);
    //Console.println("Republish to kafka complete!!!", data.count());
  }

}
