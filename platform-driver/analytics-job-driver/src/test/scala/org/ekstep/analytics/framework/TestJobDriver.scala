package org.ekstep.analytics.framework

import org.scalatest._
import org.ekstep.ilimi.analytics.framework.JobDriver
import org.ekstep.ilimi.analytics.framework.JobConfig
import org.ekstep.ilimi.analytics.framework.util.JSONUtils
import org.ekstep.ilimi.analytics.framework.Fetcher
import org.ekstep.ilimi.analytics.framework.Query
import org.ekstep.ilimi.analytics.framework.Filter
import org.ekstep.ilimi.analytics.framework.Dispatcher
import org.ekstep.ilimi.analytics.framework.conf.AppConf
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * @author Santhosh
 */
class TestJobDriver extends FlatSpec with Matchers {

    "TestJobDriver" should "successfully test the job driver" in {

        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "IN", Option(Array("OE_ASSESS", "OE_START", "OE_END", "OE_LEVEL_SET"))))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            Option("TestJobDriver"))

        noException should be thrownBy {
            val baos = new ByteArrayOutputStream
            val ps = new PrintStream(baos)
            Console.setOut(ps);
            JobDriver.main("batch", JSONUtils.serialize(jobConfig));
            baos.toString should include ("(Total Events Size,1699)");
            baos.close()
        }
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
    
    it should "thrown an exception if unable to parse the config file" in {
        a[Exception] should be thrownBy {
            JobDriver.main("streaming", JSONUtils.serialize(""));
        }
    }

}