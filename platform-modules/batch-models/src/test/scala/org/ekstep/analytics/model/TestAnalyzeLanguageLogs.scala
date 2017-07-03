package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher

case class BEEventEks(rid: Option[String], duration: Option[Int], status: Option[Int], method: Option[String])
case class BEEventEdata(eks: BEEventEks)
case class BEEvent(eid: String, edata: BEEventEdata)

case class LETransactionData(addedRelations: Array[AnyRef], removedRelations: Array[AnyRef], properties: Map[String, AnyRef])
case class LearningEvent(objectType: String, transactionData: LETransactionData)

class TestAnalyzeLanguageLogs extends SparkSpec(null) {

    "TestAnalyzeLanguageLogs" should "generate access summaries" in {

        val rdd = loadFile[BEEvent]("/Users/santhosh/ekStep/telemetry_dump/2017-06-19-backend-events.json");
        println(rdd.count());
        val accessEvents = rdd.filter { x => x.eid.equals("BE_ACCESS") };
        println("Total Access Events:" + accessEvents.count());
        val metrics = accessEvents.groupBy { x => (x.edata.eks.rid.getOrElse("UNKNOWN"), x.edata.eks.method.getOrElse("UNKNOWN")) }.mapValues { x =>
            val durations = x.map { x => x.edata.eks.duration.getOrElse(0) };
            Array(x.size, durations.min, durations.max, CommonUtil.roundDouble(durations.sum.toDouble/durations.size, 2)).mkString(",");
        }
        println("rid, method, count, mix, max, average");
        val result = metrics.map(f =>"" + f._1._1 + "," + f._1._2 + "," + f._2);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "output/backend-events-metrics-20170619.csv")), result);
    }
    
    it should "generate transaction summaries" in {
        val rdd = loadFile[LearningEvent]("/Users/santhosh/ekStep/telemetry_dump/2017-06-19-learning-events.json");
        println(rdd.count());
        println("Total Learning Events:" + rdd.count());
        val metrics = rdd.groupBy { x => x.objectType }.mapValues { x =>
            val addedRelations = x.filter { x => x.transactionData.addedRelations != null }.filter { x => x.transactionData.addedRelations.size > 0 }.size;
            val deletedRelations = x.filter { x => x.transactionData.removedRelations != null }.filter { x => x.transactionData.removedRelations.size > 0 }.size;
            val updatedProperties = x.filter { x => x.transactionData.properties != null }.filter { x => x.transactionData.properties.size > 0 }.size;
            Array(x.size, addedRelations, deletedRelations, updatedProperties).mkString(",");
        }
        println("object type, count, relations added?, relations removed?, props updated?");
        val result = metrics.map(f =>"" + f._1 + "," + f._2);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "output/learning-events-metrics-20170619.csv")), result);
    }
}