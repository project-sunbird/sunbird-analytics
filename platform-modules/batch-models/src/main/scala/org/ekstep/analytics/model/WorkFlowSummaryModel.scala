package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework._

case class WorkflowInput(sessionKey: WorkflowIndex, events: Buffer[V3Event]) extends AlgoInput
case class WorkflowOutput(index: WorkflowIndex, summaries: Buffer[MeasuredEvent]) extends AlgoOutput
case class WorkflowIndex(did: String, channel: String, pdataId: String)

object WorkFlowSummaryModel extends IBatchModelTemplate[V3Event, WorkflowInput, MeasuredEvent, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.WorkFlowSummaryModel"
    override def name: String = "WorkFlowSummaryModel"

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkflowInput] = {

        val defaultPDataId = V3PData(AppConf.getConfig("default.consumption.app.id"), Option("1.0"))
        data.map { x => (WorkflowIndex(x.context.did.getOrElse(""), x.context.channel, x.context.pdata.getOrElse(defaultPDataId).id), Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => WorkflowInput(x._1, x._2) }
    }
    override def algorithm(data: RDD[WorkflowInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        val events = data.map { x => x.events }.flatMap { x => x }.filter(f => f.`object`.isDefined)
        val gameList = events.map { x => x.`object`.get.id }.distinct().collect();
        JobLogger.log("Fetching the Content and Item data from Learning Platform")
        val contents = ContentAdapter.getAllContent();
        val itemData = Map[String, Item]() //getItemData(contents, gameList, "v2");
        val itemMapping = sc.broadcast(itemData);
        var summEvents: Buffer[MeasuredEvent] = Buffer();
        var summEventsB = sc.broadcast(summEvents);

        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];
        val sessionBreakTime = config.getOrElse("sessionBreakTime", 30).asInstanceOf[Int];

        data.foreach{ f =>
            val sortedEvents = f.events.sortBy { x => x.ets }
            var rootSummary: org.ekstep.analytics.util.Summary = null
            var currSummary: org.ekstep.analytics.util.Summary = null
            var prevEvent: V3Event = sortedEvents.head

            sortedEvents.foreach{ x =>

                val diff = CommonUtil.getTimeDiff(prevEvent.ets, x.ets).get
                if(diff > (sessionBreakTime * 60)) {
                    if(currSummary != null && !currSummary.isClosed){
                        val clonedRootSummary = currSummary.deepClone()
                        clonedRootSummary.close(summEvents, config)
                        summEventsB.value ++= clonedRootSummary.summaryEvents
                        clonedRootSummary.clearAll()
                        rootSummary = clonedRootSummary
                        currSummary = if(clonedRootSummary.CHILDREN.size > 0) clonedRootSummary.getLeafSummary else clonedRootSummary
                    }
                }
                prevEvent = x
                (x.eid) match {

                    case ("START") =>
                        if (rootSummary == null || rootSummary.isClosed) {
                            if(currSummary != null && !currSummary.isClosed){
                                currSummary.close(summEvents, config);
                                summEventsB.value ++= currSummary.summaryEvents;
                            }
                            rootSummary = new org.ekstep.analytics.util.Summary(x)
                            currSummary = rootSummary
                        }
                        else if (currSummary == null || currSummary.isClosed) {
                            currSummary = new org.ekstep.analytics.util.Summary(x)
                            if (!currSummary.checkSimilarity(rootSummary)) rootSummary.addChild(currSummary)
                        }
                        else {
                            val tempSummary = currSummary.checkStart(x.edata.`type`, Option(x.edata.mode), currSummary.summaryEvents, config)
                            if (tempSummary == null) {
                                val newSumm = new org.ekstep.analytics.util.Summary(x)
                                if (!currSummary.isClosed) {
                                    currSummary.addChild(newSumm)
                                    newSumm.addParent(currSummary, idleTime, itemMapping.value)
                                }
                                currSummary = newSumm
                            }
                            else {
                                if(currSummary.PARENT != null) {
                                    summEventsB.value ++= currSummary.PARENT.summaryEvents
                                }
                                else summEventsB.value ++= currSummary.summaryEvents
                                currSummary = new org.ekstep.analytics.util.Summary(x)
                            }
                        }
                    case ("END") =>
                        // Check if first event is END event, currSummary = null
                        if(currSummary != null) {
                            val parentSummary = currSummary.checkEnd(x, idleTime, itemMapping.value, config)
                            if (!currSummary.isClosed) {
                                currSummary.add(x, idleTime, itemMapping.value)
                                currSummary.close(summEvents, config);
                            }
                            summEventsB.value ++= currSummary.summaryEvents
                            currSummary = parentSummary
                        }
                    case _ =>
                        if (currSummary != null && !currSummary.isClosed) {
                            currSummary.add(x, idleTime, itemMapping.value)
                        }
                        else{
                            currSummary = new org.ekstep.analytics.util.Summary(x)
                            currSummary.updateType("app")
                        }
                }
            }
            if(currSummary != null && !currSummary.isClosed){
                currSummary.close(currSummary.summaryEvents, config)
                summEventsB.value ++= currSummary.summaryEvents
            }
        }
        sc.parallelize(summEventsB.value);
    }
    override def postProcess(data: RDD[MeasuredEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data
    }
}