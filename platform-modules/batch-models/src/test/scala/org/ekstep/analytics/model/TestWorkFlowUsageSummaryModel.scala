package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent

class TestWorkFlowUsageSummaryModel extends SparkSpec(null) {
  
    "WorkFlowUsageSummaryModel" should "generate workflow usage summary events" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/workflow-usage-summary/test-data1.log");
        val rdd2 = WorkFlowUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();
        
//        me.foreach(f => println(JSONUtils.serialize(f)))
    }
}