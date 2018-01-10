package org.ekstep.analytics.model

import org.ekstep.analytics.framework.V3Event
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.V3PData
import org.ekstep.analytics.framework.util.JSONUtils

class TestWorkFlowSummaryModel extends SparkSpec {
    it should "test data" in {
        val data = loadFile[V3Event]("src/test/resources/workflow-summary/test-data1.log")
        val out = WorkFlowSummaryModel.execute(data, None)
        println("output count: ", out.count)
        out.foreach(f => println(JSONUtils.serialize(f)))
        
        
//        println("output map size: " + mappedOut.size)
//        mappedOut.foreach(f => println(" key: ", f._1, " bufferCount: ", f._2.size, " firstEvent: ", f._2.head.eid, " LastEvent: ", f._2.last.eid))
        
        
        
        
        
//        val didEvents = data.map { x => (x.context.did.getOrElse(""), Buffer(x)) }
//        val perDidEvents = didEvents.reduceByKey((a, b) => a ++ b)
//        perDidEvents.foreach { x =>
//            println(x._1)
//            x._2.sortBy { x => x.ets }.foreach { x =>
//                
//                val pdata = x.context.pdata.getOrElse(V3PData("")).id
//                println("--------- "+ x.eid +", " + x.edata.`type`+", "+ x.context.env+", " + pdata)
//            }
//        }
    }

}