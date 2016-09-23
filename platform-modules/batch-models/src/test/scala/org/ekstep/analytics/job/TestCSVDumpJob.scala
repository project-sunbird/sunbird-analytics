package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.job.summarizer.CSVDumpJob

class TestCSVDumpJob extends SparkSpec(null) {

    "CSVDumpJob" should "execute csv dump job on raw telemetry data" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.FieldExtractor", Option(Map("eid" -> "OE_ASSESS", "headers" -> "eid,ts,gameId", "fields" -> "eid,ts,gdata.id")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestCSVDumpJob"), Option(false))
        CSVDumpJob.main(JSONUtils.serialize(config))(Option(sc));
    }
    
    it should "execute csv dump job on derived telemetry data" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.FieldExtractor", Option(Map("eid" -> "ME_SESSION_SUMMARY", "headers" -> "eid,ts", "fields" -> "eid,ets")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestCSVDumpJob"), Option(false))
        CSVDumpJob.main(JSONUtils.serialize(config))(Option(sc));
    }
    
    it should "execute csv dump job on derived telemetry data even when no eid is passed" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.FieldExtractor", Option(Map("headers" -> "eid,ts", "fields" -> "eid,ets")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestCSVDumpJob"), Option(false))
        CSVDumpJob.main(JSONUtils.serialize(config))(Option(sc));
    }
    
    ignore should "execute csv dump job job fetching data from s3 prod file" in {

        val config = JobConfig(Fetcher("S3", None, Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-06-20"), Option("2016-07-27"), None, None, None, None, None, None)))), None, null, "org.ekstep.analytics.model.FieldExtractor", Option(Map("eid" -> "ME_SESSION_SUMMARY", "headers" -> "eid,gameid,timespent,tags", "fields" -> "eid,dimensions.gdata.id,edata.eks.timeSpent,tags")), Option(Array(Dispatcher("file", Map("file" -> "dump_ss_20062016_27072016.csv")))), Option(10), Option("TestCSVDumpJob"), Option(false));
        CSVDumpJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}