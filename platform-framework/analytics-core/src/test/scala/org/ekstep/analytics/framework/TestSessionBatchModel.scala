package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
class TestSessionBatchModel extends SparkSpec {
  
    "SessionBatchModel" should "group data by game session" in {
        
        val rdd = SampleModel.execute(sc, events, None);
        rdd.count should be (516);
        
        val rdd1 = loadFile[TelemetryEventV2]("src/test/resources/sample_telemetry.log");
        val rdd2 = SampleModelV2.execute(sc, rdd1, None);
        rdd2.count should be (516);
    }
}