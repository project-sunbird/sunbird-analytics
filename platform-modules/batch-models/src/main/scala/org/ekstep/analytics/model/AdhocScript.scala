package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils

object AdhocScript {
  
    def main(args: Array[String]): Unit = {
        implicit val sc = CommonUtil.getSparkContext(10, "AdhocScript");
        val queries = Option(Array(Query(Option("ekstep-prod-data-store"), Option("1970-01-01"), None, None)));
        val data = DataFetcher.fetchBatchData[Map[String, AnyRef]](Fetcher("S3", None, queries));
        Console.println("Count:", data.count());
    }
}