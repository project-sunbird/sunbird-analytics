package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._

class TestDeviceRecommendationModel extends SparkSpec(null) {
  
    ignore should "generate libsvm files" in {

        val me = DeviceRecommendationModel.execute(null, None);
    }
}