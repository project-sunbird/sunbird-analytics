package org.ekstep.analytics.model

import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext

case class DeviceEvent(did: String, events: Buffer[V3Event]) extends AlgoInput
case class Metrics() extends AlgoOutput

object WorkFlowSummaryModel extends IBatchModelTemplate[V3Event, DeviceEvent, Metrics, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.WorkFlowSummaryModel"
    override def name: String = "WorkFlowSummaryModel"

    private def arrangeWorkflowData(events: Buffer[V3Event]): Map[String, Buffer[V3Event]] = {

        val sortedEvents = events.sortBy { x => x.ets }

        var workFlowData = Map[String, Buffer[V3Event]]();
        var tmpArr = Buffer[V3Event]();

        var appKey = ""
        var sessionKey = ""
        var playerKey = ""

        sortedEvents.foreach { x =>

            (x.eid, x.edata.`type`) match {
                case ("START", "app") =>

                    if (appKey.isEmpty()) {
                        appKey = "app" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                    } else if (workFlowData.contains(appKey)) {
                        // closing app workflow
                        workFlowData += (appKey -> (workFlowData.get(appKey).get ++ tmpArr))

                        // closing session workflow if any
                        if (sessionKey.nonEmpty && workFlowData.contains(sessionKey))
                            workFlowData += (sessionKey -> (workFlowData.get(sessionKey).get ++ tmpArr))
                        else if (sessionKey.nonEmpty && !workFlowData.contains(sessionKey))
                            workFlowData += (sessionKey -> tmpArr)

                        // closing player workflow if any
                        if (playerKey.nonEmpty && !workFlowData.contains(playerKey))
                            workFlowData += (appKey -> tmpArr)

                        //adding new app-workflow
                        appKey = "app" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;

                    } else {
                        // closing app workflow
                        workFlowData += (appKey -> tmpArr)

                        appKey = "app" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                    }

                case ("END", "app")       =>
                    // closing app workflow
                    workFlowData += (appKey -> (workFlowData.get(appKey).get ++ tmpArr))
                    tmpArr = Buffer[V3Event]();
                    
                case ("START", "session") =>
                case ("END", "session")   =>

                case ("START", "editor")  =>
                case ("END", "editor")    =>

                case ("START", "player")  =>
                case ("END", "player")    =>

                case _ =>
                    tmpArr += x
            }
        }

        null
    }

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceEvent] = {

        val deviceEvents = data.map { x => (x.context.did.getOrElse(""), Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => DeviceEvent(x._1, x._2) }
        deviceEvents;
    }
    override def algorithm(data: RDD[DeviceEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Metrics] = {
        null
    }
    override def postProcess(data: RDD[Metrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        null
    }
}