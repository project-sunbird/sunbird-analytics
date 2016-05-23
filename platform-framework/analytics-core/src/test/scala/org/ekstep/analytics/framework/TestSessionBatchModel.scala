package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
class TestSessionBatchModel extends SparkSpec {
  
    "SessionBatchModel" should "group data by game session" in {
        
        val rdd = SampleModel.execute(events, None);
        rdd.count should be (134);
        
    }
}