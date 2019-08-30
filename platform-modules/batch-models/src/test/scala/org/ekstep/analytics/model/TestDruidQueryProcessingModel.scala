package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}

class TestDruidQueryProcessingModel extends SparkSpec(null) {

    "DruidQueryProcessingModel" should "execute multiple queries and generate csv reports on multiple dimensions with dynamic interval" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2019-08-01", "2019-08-05")), None, Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), List("id", "dims", "date"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute multiple queries and generate csv reports on single dimension" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute multiple queries and generate single json report" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "throw exception if query has different dimensions" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(("device_loc_city", "city"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }

    it should "throw exception if query does not have intervals" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, None, Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }

    it should "execute report and generate multiple csv reports" in {
        val scansQuery1 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery1 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig1 = ReportConfig("data_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery1), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery1)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", Option("scans"), List("total_scans"), List("state", "producer_id")), OutputConfig("csv", Option("sessions"), List("total_sessions", "total_ts"), List("state", "producer_id"), List("id", "dims", "date"))))
        val strConfig1 = JSONUtils.serialize(reportConfig1)

        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig1), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute weekly report and generate csv reports" in {
        val scansQuery2 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(("device_loc_state", "state"), ("context_pdata_id", "producer_id"))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery2 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(("device_loc_state", "state"), ("dimensions_pdata_id", "producer_id"))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig2 = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastWeek"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery2), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery2)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig2 = JSONUtils.serialize(reportConfig2)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute weekly report for QR scans and generate csv reports" in {
        val totalQRscansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")))))
        val reportConfig2 = ReportConfig("data_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", totalQRscansQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans"), List(OutputConfig("csv", Option("scans"), List("total_scans"))))
        val strConfig2 = JSONUtils.serialize(reportConfig2)

        //        val reportConfig = """{"id":"data_metrics","queryType":"groupBy","dateRange":{"staticInterval":"LastDay","granularity":"day"},"metrics":[{"metric":"totalQrScans","label":"Total QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}},{"metric":"totalSuccessfulScans","label":"Total Successful QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_successful_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"greaterThan","dimension":"edata_size","value":0},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}},{"metric":"totalfailedQRScans","label":"Total Failed QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_failed_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"equals","dimension":"edata_size","value":0},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}}],"labels":{"state":"State","total_sessions":"Number of Content Plays","producer_id":"Producer","total_scans":"Total Number of QR Scans","total_successful_scans":"Total Number Of Successful QR Scans","total_failed_scans":"Total Number Of Failed QR Scans"},"output":[{"type":"csv","label":"QR Scans","metrics":["total_scans","total_successful_scans","total_failed_scans"],"dims":[],"fileParameters":["id","date"]}]}"""
        println(JSONUtils.deserialize[Map[String, AnyRef]](strConfig2))
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }
}
