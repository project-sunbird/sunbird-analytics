package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._

class TestREContext extends SparkSpec(null) {
  
    ignore should "generate libsvm files" in {

        val me = REContext.execute(null, None);
    }
}