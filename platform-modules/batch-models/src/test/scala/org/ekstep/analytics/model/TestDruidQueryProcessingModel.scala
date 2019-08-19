package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.util.JSONUtils

class TestDruidQueryProcessingModel extends SparkSpec(null) {

    "DruidQueryProcessingModel" should "execute multiple queries and generate csv reports on multiple dimensions" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation("total_scans", "count", None))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation("total_sessions", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastWeek"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), "$id/$dims.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute multiple queries and generate csv reports on multiple dimensions with dynamic interval" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation("total_scans", "count", None))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation("total_sessions", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2019-08-01", "2019-08-05")), None, Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), "$id/$dims.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute multiple queries and generate csv reports on single dimension" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation("total_scans", "count", None))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation("total_sessions", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", List("total_scans", "total_sessions", "total_ts"), List("state"), "$id/$dims.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute multiple queries and generate single json report" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation("total_scans", "count", None))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation("total_sessions", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), "$id/$dims.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "throw exception if query has different dimensions" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation("total_scans", "count", None))), Option(List(("device_loc_city", "city"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation("total_sessions", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), "$id/$dims.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }

    it should "throw exception if query does not have intervals" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation("total_scans", "count", None))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation("total_sessions", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, None, Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), "$id/$dims.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }
}
