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
case class WorkflowOutput(index: WorkflowIndex, summaries: Buffer[org.ekstep.analytics.util.Summary]) extends AlgoOutput
case class WorkflowIndex(did: String, channel: String, pdataId: String)

object WorkFlowSummaryModel extends IBatchModelTemplate[V3Event, WorkflowInput, WorkflowOutput, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.WorkFlowSummaryModel"
    override def name: String = "WorkFlowSummaryModel"

    /**
     *
     */
    private def getItemData(contents: Array[Content], games: Array[String], apiVersion: String): Map[String, Item] = {

        val gameIds = contents.map { x => x.id };
        val codeIdMap: Map[String, String] = contents.map { x => (x.metadata.get("code").get.asInstanceOf[String], x.id) }.toMap;
        val contentItems = games.map { gameId =>
            {
                if (gameIds.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(gameId, apiVersion))
                } else if (codeIdMap.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(codeIdMap.get(gameId).get, apiVersion))
                } else {
                    null;
                }
            }
        }.filter(x => x != null).filter(_._2 != null).toMap;

        if (contentItems.size > 0) {
            contentItems.map(f => {
                f._2.map { item =>
                    (item.id, item)
                }
            }).reduce((a, b) => a ++ b).toMap;
        } else {
            Map[String, Item]();
        }
    }

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkflowInput] = {

        val defaultPDataId = V3PData(AppConf.getConfig("default.consumption.app.id"), Option("1.0"))
        data.map { x => (WorkflowIndex(x.context.did.getOrElse(""), x.context.channel, x.context.pdata.getOrElse(defaultPDataId).id), Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => WorkflowInput(x._1, x._2) }
    }
    override def algorithm(data: RDD[WorkflowInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkflowOutput] = {

        val events = data.map { x => x.events }.flatMap { x => x }.filter(f => f.`object`.isDefined)
        val gameList = events.map { x => x.`object`.get.id }.distinct().collect();
        JobLogger.log("Fetching the Content and Item data from Learning Platform")
        val contents = ContentAdapter.getAllContent();
        val itemData = getItemData(contents, gameList, "v2");
        val itemMapping = sc.broadcast(itemData);

        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];

        val summaryOut = data.map { x => (x.sessionKey, x.events)}.mapValues { f =>
            val firstEvent = f.head
            val lastEvent = f.last
            var prevSummary: org.ekstep.analytics.util.Summary = null
            var summary: Buffer[org.ekstep.analytics.util.Summary] = Buffer();
            var unclosedSummaries: Buffer[org.ekstep.analytics.util.Summary] = Buffer();

            f.foreach{ x =>
                (x.eid) match {

                    case ("START") =>
                        if(summary.size == 0) {
                            prevSummary = new org.ekstep.analytics.util.Summary(x.edata.`type` + "_" + x.edata.mode, x);
                        }
                        else if(prevSummary.checkSimilarity(x.edata.`type` + "_" + x.edata.mode)) {
                            prevSummary.close();
                            summary += prevSummary
                            prevSummary = new org.ekstep.analytics.util.Summary(x.edata.`type` + "_" + x.edata.mode, x);
                        }
                        else {
                            if(!prevSummary.isClosed){
                                val newSummary = new org.ekstep.analytics.util.Summary(x.edata.`type` + "_" + x.edata.mode, x);
                                prevSummary.addChild(newSummary)
                                newSummary.setParent(prevSummary)
                                unclosedSummaries += prevSummary
                                prevSummary = newSummary
                            }
                            else {
                                val newSummary = new org.ekstep.analytics.util.Summary(x.edata.`type` + "_" + x.edata.mode, x);
                                newSummary.setParent(prevSummary.getParent())
                                // check:
//                                prevSummary.getParent().addChild(newSummary)
//                                summary += prevSummary
                                prevSummary = newSummary
                            }
                        }
                    case ("END") =>
                        if(prevSummary.checkSimilarity(x.edata.`type` + "_" + x.edata.mode)) {
                            prevSummary.add(x, idleTime, itemMapping.value);
                            prevSummary.close();
                            summary += prevSummary
                        }
                        else {
                            unclosedSummaries.foreach { f =>
                                if(f.checkSimilarity(x.edata.`type` + "_" + x.edata.mode)) {
                                    f.add(x, idleTime, itemMapping.value);
                                    f.close();
                                    summary += f;
                                }
                            }
                        }
                    case _ =>
                        if(StringUtils.equals(firstEvent.mid, x.mid))
                            prevSummary = new org.ekstep.analytics.util.Summary("app_" + x.edata.mode, x);
                        else if(StringUtils.equals(lastEvent.mid, x.mid)) {
                            prevSummary.add(x, idleTime, itemMapping.value);
                            prevSummary.close();
                        }
                        else
                            prevSummary.add(x, idleTime, itemMapping.value)
                }
            }
            if(unclosedSummaries.size > 0) {
                unclosedSummaries.foreach { f =>
                        f.close();
                        summary += f;
                    }
            }
            summary;
        }
        summaryOut.foreach(f => println(f._1, f._2.size))
        summaryOut.map(x => WorkflowOutput(x._1, x._2))
    }
    override def postProcess(data: RDD[WorkflowOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { f =>
            val index = f.index
            f.summaries.map { session =>
                val dt_range = DtRange(session.start_time, session.end_time)
                val mid = CommonUtil.getMessageId("ME_WORKFLOW_SUMMARY", session.uid, "SESSION", dt_range, "NA", Option(index.pdataId), Option(index.channel));
                val interactEventsPerMin: Double = if (session.interact_events_count == 0 || session.time_spent == 0) 0d
                    else if (session.time_spent < 60.0) session.interact_events_count.toDouble
                    else BigDecimal(session.interact_events_count / (session.time_spent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
                val syncts = CommonUtil.getEventSyncTS(session.last_event)
                val measures = Map("start_time" -> session.start_time,
                    "end_time" -> session.end_time,
                    "time_diff" -> session.time_diff,
                    "time_spent" -> session.time_spent,
                    "telemetry_version" -> session.telemetry_version,
                    "mode" -> session.mode,
                    "item_responses" -> session.item_responses,
                    "interact_events_count" -> session.interact_events_count,
                    "interact_events_per_min" -> interactEventsPerMin,
                    "env_summary" -> session.env_summary,
                    "events_summary" -> session.events_summary,
                    "page_summary" -> session.page_summary);
                MeasuredEvent("ME_WORKFLOW_SUMMARY", System.currentTimeMillis(), syncts, meEventVersion, mid, session.uid, null, None, None,
                    Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "WorkflowSummarizer").asInstanceOf[String])), None, "SESSION", dt_range),
                    Dimensions(None, Option(index.did), None, None, None, None, Option(PData(index.pdataId, "1.0")), None, None, None, None, None, session.content_id, None, None, Option(session.sid), None, None, None, None, None, None, None, None, None, None, Option(index.channel), Option(session.session_type)),
                    MEEdata(measures), session.etags);
            }
        }.flatMap(x => x)
    }
}