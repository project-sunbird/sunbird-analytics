package org.ekstep.analytics.transformer

import org.ekstep.analytics.model.SparkSpec

class TestDeviceRecommendationTransformer extends SparkSpec {
  
    "DeviceRecommendationTransformer" should "perform outlier" in {
        
        ContentUsageTransformer.name() should be ("DeviceRecommendationTransformer")
        val in = Array(("id", 0.0), ("id", 10.0), ("id", 20.0), ("id", 100.0), ("id", 1000.0), ("id", 2000.0), ("id", -3000.0), ("id", 2500.0))
        val inRDD = sc.parallelize(in)
        ContentUsageTransformer.outlierTreatment(inRDD)
    }
}