package com.ekstep.ilimi.analytics.framework

import org.ekstep.ilimi.analytics.framework.BaseSpec
import org.ekstep.ilimi.analytics.framework.JobDriver
import org.ekstep.ilimi.analytics.framework.JobConfig
import org.ekstep.ilimi.analytics.framework.util.JSONUtils
import org.ekstep.ilimi.analytics.framework.Fetcher
import org.ekstep.ilimi.analytics.framework.Query
import org.ekstep.ilimi.analytics.framework.Filter
import org.ekstep.ilimi.analytics.framework.Dispatcher
import org.ekstep.ilimi.analytics.framework.conf.AppConf


/**
 * @author Santhosh
 */
class TestJobDriver extends BaseSpec {

    "TestJobDriver" should "successfully execute batch job driver using local file" in {
        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), 
            Option(Array[Filter](Filter("eventId", "IN", Option(Array("OE_ASSESS", "OE_START", "OE_END", "OE_LEVEL_SET"))))), 
            None, 
            "org.ekstep.analytics.model.GenericScreenerSummary", 
            Option(Map("contentId" -> "numeracy_377")), 
            Option(Array(Dispatcher("console", Map("test" -> "test")), Dispatcher("kafka", Map("test" -> "test")))), 
            Option(2), 
            Option("TestJobDriver"))
        JobDriver.main("batch", JSONUtils.serialize(jobConfig));
    }

    it should "invoke stream job driver" in {
        val jobConfig = JobConfig(Fetcher("stream", None, None), None, None, "", None, None, None, None)
        JobDriver.main("streaming", JSONUtils.serialize(jobConfig));
    }

    it should "thrown an exception if unknown job type is found" in {
        val jobConfig = JobConfig(Fetcher("stream", None, None), None, None, "", None, None, None, None)
        a[Exception] should be thrownBy {
            JobDriver.main("xyz", JSONUtils.serialize(jobConfig));
        }
    }

}