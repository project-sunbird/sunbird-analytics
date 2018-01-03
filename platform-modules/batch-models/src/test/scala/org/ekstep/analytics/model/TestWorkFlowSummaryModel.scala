package org.ekstep.analytics.model

import org.ekstep.analytics.framework.V3Event
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.V3PData

class TestWorkFlowSummaryModel extends SparkSpec {
    it should "test data" in {
        val data = loadFile[V3Event]("/Users/amitBehera/backup/data/workflowExp/2017-12-22-1513990631006.json")
        val didEvents = data.map { x => (x.context.did.getOrElse(""), Buffer(x)) }
        val perDidEvents = didEvents.reduceByKey((a, b) => a ++ b)
        perDidEvents.foreach { x =>
            println(x._1)
            x._2.sortBy { x => x.ets }.foreach { x =>
                
                val pdata = x.context.pdata.getOrElse(V3PData("")).id
                println("--------- "+ x.eid +", " + x.edata.`type`+", "+ x.context.env+", " + pdata)
            }
        }
    }

}