package org.ekstep.analytics.model

import org.ekstep.analytics.framework.V3Event
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.V3PData
import org.ekstep.analytics.framework.util.JSONUtils

class TestWorkFlowSummaryModel extends SparkSpec {
  
    it should "generate 9 workflow summary" in {
        val data = loadFile[V3Event]("src/test/resources/workflow-summary/test-data1.log")
        val out = WorkFlowSummaryModel.execute(data, None)
//        println("output count: ", out.count)
//        out.foreach(f => println(JSONUtils.serialize(f)))
        out.count() should be(9)
    }
    
    it should "generate 5 workflow summary" in {
        val data = loadFile[V3Event]("src/test/resources/workflow-summary/test-data2.log")
        val out = WorkFlowSummaryModel.execute(data, None)
//        println("output count: ", out.count)
        out.foreach(f => println(JSONUtils.serialize(f)))
        out.count() should be(5)
    }

}