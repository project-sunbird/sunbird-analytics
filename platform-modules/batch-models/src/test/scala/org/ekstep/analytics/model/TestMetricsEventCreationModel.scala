package org.ekstep.analytics.model

import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent

class TestMetricsEventCreationModel extends SparkSpec(null) {
  
    "MetricsEventCreationModel" should "execute MetricsEventCreationModel successfully" in {

        val data = sc.parallelize(List(""))
        val rdd2 = MetricsEventCreationModel.execute(data, Option(Map("start_date" -> 1497551400000L.asInstanceOf[AnyRef], "end_date" -> 1497616200000L.asInstanceOf[AnyRef])));
    }
}