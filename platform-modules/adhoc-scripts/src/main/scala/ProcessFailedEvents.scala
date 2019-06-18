import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import java.lang.Double

object ProcessFailedEvents extends optional.Application {

    def main(topic: String, brokerList: String, fromDate: String, toDate: String)(implicit sc: SparkContext = null) {

        if (null == sc) {
            implicit val sc: SparkContext = CommonUtil.getSparkContext(10, "ProcessFailedEvents");
            correctData(topic, brokerList, fromDate, toDate);
        }
        else {
            correctData(topic, brokerList, fromDate, toDate);
        }


    }

    def correctData(topic: String, brokerList: String, fromDate: String, toDate: String)(implicit sc: SparkContext): Unit = {
//        val queryConfig = """{"type":"local","queries":[{"file":"/Users/sowmya/test-duration-conversion.txt"}]}""";
//        val rdd = DataFetcher.fetchBatchData[Map[String, AnyRef]](JSONUtils.deserialize[Fetcher](queryConfig));

        val queries = Option(Array(Query(Option("telemetry-data-store"), Option("failed/"), Option(fromDate), Option(toDate))));
        val rdd = DataFetcher.fetchBatchData[Map[String, AnyRef]](Fetcher("azure", None, queries))

        val validationFailures = rdd.filter{ f =>
            "TelemetryValidator".equals(f.getOrElse("metadata", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].get("src").getOrElse(""))
        }
        val filteredData = validationFailures.filter{f =>
            f.getOrElse("edata", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].get("duration").getOrElse(0.0).isInstanceOf[String]
        }
        println("total failed count: " + rdd.count + " validation failure count: " + validationFailures.count + " duration issue count: " + filteredData.count)
        val correctedData = filteredData.map{f =>
            val edata = f.getOrElse("edata", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
            val duration = edata.get("duration").get
            val correctedDuration = if(duration.isInstanceOf[String]) java.lang.Double.parseDouble(duration.asInstanceOf[String]) else duration.asInstanceOf[Double]
            val correctedEdata = edata ++ Map("duration" -> correctedDuration.asInstanceOf[Double])
            // add corrected edata and remove metadata
            val finalData = f ++ Map("edata" -> correctedEdata) ++ Map("metadata" -> null)
//            println("final data: " + JSONUtils.serialize(finalData))
            JSONUtils.serialize(finalData)
        }
        val config = Map("topic" -> topic, "brokerList" -> brokerList)
        OutputDispatcher.dispatch(Dispatcher("kafka", config), correctedData);
        Console.println("Republish to kafka complete!!!");
    }
}

object FixAndProcessFailedEvents extends optional.Application {

    def main(topic: String, brokerList: String, fromDate: String, toDate: String): Unit = {
        ProcessFailedEvents.main(topic, brokerList, fromDate, toDate);
    }

}