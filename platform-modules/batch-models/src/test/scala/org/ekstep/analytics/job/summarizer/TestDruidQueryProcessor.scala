package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestDruidQueryProcessor extends SparkSpec(null) {

    "DruidQueryProcessor" should "execute DruidQueryProcessor job and won't throw any Exception" in {

        //val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation("count", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List("dimensions_channel", "dimensions_sid", "dimensions_pdata_id", "dimensions_type", "dimensions_mode", "dimensions_did", "object_id", "content_board")), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation("count", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List("dimensions_pdata_id", "dimensions_type")), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val config = JobConfig(Fetcher("druid", None, None, Option(contentPlaysQuery)), null, null, "org.ekstep.analytics.model.DruidQueryProcessingModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> true.asInstanceOf[AnyRef])))), Option(10), Option("TestDruidQueryProcessor"), Option(true))
        DruidQueryProcessor.main(JSONUtils.serialize(config))(Option(sc));
    }
}
