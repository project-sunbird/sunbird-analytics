package org.ekstep.analytics.framework

import org.scalatest._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */

object TestModel2 extends IBatchModel[MeasuredEvent] with Serializable {
    
    def execute(events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        events.map { x => JSONUtils.serialize(x) };
    }
  
}

object TestModel3 extends IBatchModel[TelemetryEventV2] with Serializable {
    
    def execute(events: RDD[TelemetryEventV2], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        events.map { x => JSONUtils.serialize(x) };
    }
  
}

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
            None,
            Option(true))

        noException should be thrownBy {
            val baos = new ByteArrayOutputStream
            val ps = new PrintStream(baos)
            Console.setOut(ps);
            implicit val sc: SparkContext = null;
            JobDriver.run[Event]("batch", JSONUtils.serialize(jobConfig), new TestModel);
            baos.toString should include ("(Total Events Size,1699)");
            baos.close()
        }
    }

    it should "invoke stream job driver" in {
        val jobConfig = JobConfig(Fetcher("stream", None, None), None, None, "", None, None, None, None)
        implicit val sc: SparkContext = null;
        JobDriver.run("streaming", JSONUtils.serialize(jobConfig), new TestModel);
    }

    it should "thrown an exception if unknown job type is found" in {
        val jobConfig = JobConfig(Fetcher("stream", None, None), None, None, "", None, None, None, None)
        a[Exception] should be thrownBy {
            implicit val sc: SparkContext = null;
            JobDriver.run("xyz", JSONUtils.serialize(jobConfig), new TestModel);
        }
    }
    
    it should "thrown an exception if unable to parse the config file" in {
        a[Exception] should be thrownBy {
            implicit val sc: SparkContext = null;
            JobDriver.run("streaming", JSONUtils.serialize(""), new TestModel);
        }
    }
    
    it should "fetch the app name from job config model if the app name is not specified" in {
        
        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "EQ", Option("OE_START")))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            None)

        noException should be thrownBy {
            implicit val sc: SparkContext = CommonUtil.getSparkContext(1, "Test");
            JobDriver.run[Event]("batch", JSONUtils.serialize(jobConfig), new TestModel);
            CommonUtil.closeSparkContext();
        }
    }
    
    it should "run the job driver on measured event" in {
        
        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "EQ", Option("OE_START")))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            Option(true))

        noException should be thrownBy {
            implicit val sc: SparkContext = CommonUtil.getSparkContext(1, "Test");
            JobDriver.run[MeasuredEvent]("batch", JSONUtils.serialize(jobConfig), TestModel2);
            CommonUtil.closeSparkContext();
        }
    }
    
    it should "run the job driver on telemetry event v2" in {
        
        val jobConfig = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))),
            Option(Array[Filter](Filter("eventId", "EQ", Option("OE_START")))),
            None,
            "org.ekstep.analytics.framework.TestModel",
            Option(Map()),
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(8),
            None,
            Option(true))

        noException should be thrownBy {
            implicit val sc: SparkContext = CommonUtil.getSparkContext(1, "Test");
            JobDriver.run[TelemetryEventV2]("batch", JSONUtils.serialize(jobConfig), TestModel3);
            CommonUtil.closeSparkContext();
        }
    }

}