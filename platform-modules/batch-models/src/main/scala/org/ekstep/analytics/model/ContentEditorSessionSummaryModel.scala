/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.model

import java.net.URLDecoder

import scala.BigDecimal
import scala.collection.mutable.Buffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.SessionBatchModel
import java.net.URL
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.commons.validator.UrlValidator

/**
 * Case class to hold the screener summary fields
 */
case class PerPluginSummary(plugin_id: String, added_count: Int, deleted_count: Int, modified_count: Int)
case class PluginSummary(loaded_count: Int, added_count: Int, deleted_count: Int, modified_count: Int, per_plugin_summary: Iterable[PerPluginSummary])
case class SaveSummary(total_count: Int, success_count: Int, failed_count: Int)
case class CEStageSummary(added_count: Int, deleted_count: Int, modified_count: Int)

/**
 * Case class to hold the session summary input and output
 */
case class CESessionSummaryInput(sid: String, filteredEvents: Buffer[CreationEvent]) extends AlgoInput
case class CESessionSummaryOutput(uid: String, sid: String, contentId: String, client: Map[String, AnyRef], dateRange: DtRange, ss: CESessionSummary) extends AlgoOutput

/**
 * Case class to hold the screener summary
 */
class CESessionSummary(val time_spent: Double, val start_time: Long, val end_time: Long, val time_diff: Double, val load_time: Double, val interact_events_count: Int,
                       val interact_events_per_min: Double, val plugin_summary: PluginSummary, val save_summary: SaveSummary, val stage_summary: CEStageSummary, val events_summary: Iterable[EventSummary],
                       val api_calls_count: Long, val sidebar_events_count: Int, val menu_events_count: Int)

/**
 * @dataproduct
 * @summarizer
 *
 * ContentEditorSessionSummaryModel
 *
 * Functionality
 * 1. Generate app specific session summary events. This would be used to compute app usage metrics.
 * Events used - CE_API_CALL, CE_START, CE_END, CE_PLUGIN_LIFECYCLE, CE_INTERACT, CE_ERROR
 */

object ContentEditorSessionSummaryModel extends SessionBatchModel[CreationEvent, MeasuredEvent] with IBatchModelTemplate[CreationEvent, CESessionSummaryInput, CESessionSummaryOutput, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ContentEditorSessionSummaryModel"
    override def name(): String = "ContentEditorSessionSummaryModel"

    /**
     * To calculate the plugin summary per content editor session
     * @param data is the Buffer of plugin events - CE_PLUGIN_LIFECYCLE 
     * @return the case class of plugin summary
     */
    def computePluginSummary(data: Buffer[CreationEvent]): PluginSummary = {
        val loadedCount = data.filter { x => "load".equals(x.edata.eks.`type`) }.length
        val perPluginSummary = data.groupBy { x => x.edata.eks.pluginid }.map { events =>
            val pluginId = events._1
            val added = events._2.filter { x => "add".equals(x.edata.eks.`type`) }.length
            val removed = events._2.filter { x => "remove".equals(x.edata.eks.`type`) }.length
            val modified = events._2.filter { x => "modify".equals(x.edata.eks.`type`) }.length
            PerPluginSummary(pluginId, added, removed, modified)
        }
        val pluginAdded = perPluginSummary.map { x => x.added_count }.sum
        val pluginRemoved = perPluginSummary.map { x => x.deleted_count }.sum
        val pluginModified = perPluginSummary.map { x => x.modified_count }.sum

        PluginSummary(loadedCount, pluginAdded, pluginRemoved, pluginModified, perPluginSummary)
    }

    /**
     * To calculate the stage summary per content editor session
     * @param data is the Buffer of plugin events - CE_PLUGIN_LIFECYCLE
     * @return the case class of Stage Summary
     */
    def computeStageSummary(data: Buffer[CreationEvent]): CEStageSummary = CEStageSummary(data.filter { x => "add".equals(x.edata.eks.`type`) }.length, data.filter { x => "remove".equals(x.edata.eks.`type`) }.length, data.filter { x => "modify".equals(x.edata.eks.`type`) }.length)

    /**
     * To calculate the save summary per content editor session
     * @param data is the Buffer of API events - CE_API_CALL
     * @return the case class of save summary
     */
    def computeSaveSummary(data: Buffer[CreationEvent]): SaveSummary = {
        val saveEvents = data.filter { x =>
            val url = URLDecoder.decode(x.edata.eks.path) 
            if(new UrlValidator().isValid(url)) AppConf.getConfig("lp.path").equals(new URL(url).getPath) else false
        }
        SaveSummary(saveEvents.length, saveEvents.filter { x => "OK".equals(x.edata.eks.`type`) }.length, saveEvents.filter { x => "ERROR".equals(x.edata.eks.`type`) }.length)
    }

    override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CESessionSummaryInput] = {
        val filteredData = DataFilter.filter(data, Array(Filter("uid", "ISNOTEMPTY", None), Filter("eventId", "IN", Option(List("CE_API_CALL", "CE_START", "CE_END", "CE_PLUGIN_LIFECYCLE", "CE_INTERACT", "CE_ERROR")))));
        val contentSessions = getCESessions(filteredData);
        contentSessions.map { x => CESessionSummaryInput(x._1, x._2) }
    }

    override def algorithm(data: RDD[CESessionSummaryInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CESessionSummaryOutput] = {
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];
        data.map { x =>
            val events = x.filteredEvents
            val startEvent = events.head
            val endEvent = events.last
            val interactEvents = events.filter { x => x.eid.equals("CE_INTERACT") }
            val pluginEvents = events.filter { x => x.eid.equals("CE_PLUGIN_LIFECYCLE") }
            val apiEvents = events.filter { x => x.eid.equals("CE_API_CALL") }
            var tmpLastEvent: CreationEvent = null;
            val eventsWithTs = events.map { x =>
                if (tmpLastEvent == null) tmpLastEvent = x;
                val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
                tmpLastEvent = x;
                (x, if (ts > idleTime) 0 else ts)
            }

            val timeSpent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);
            val startTimestamp = startEvent.ets
            val endTimestamp = endEvent.ets
            val timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get, 2);
            val loadTime = if ("CE_START".equals(startEvent.eid)) startEvent.edata.eks.loadtimes.getOrElse("contentLoad", 0.0) else 0.0
            val noOfInteractEvents = interactEvents.length
            val interactEventsPerMin: Double = if (noOfInteractEvents == 0 || timeSpent == 0) 0d
            else if (timeSpent < 60.0) noOfInteractEvents.toDouble
            else BigDecimal(noOfInteractEvents / (timeSpent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            
            val pluginSummary = computePluginSummary(pluginEvents)
            val saveSummary = computeSaveSummary(apiEvents)
            val stageSummary = computeStageSummary(pluginEvents.filter(x => x.edata.eks.stage.nonEmpty))
            val eventSummary = events.groupBy { x => x.eid }.map(f => EventSummary(f._1, f._2.length));
            val apiCallCount = apiEvents.length
            val sideBarEventCount = interactEvents.filter { x => "sidebar".equals(x.edata.eks.subtype) }.length
            val menuEventCount = interactEvents.filter { x => "menu".equals(x.edata.eks.subtype) && "click".equals(x.edata.eks.`type`) }.length

            CESessionSummaryOutput(startEvent.uid, startEvent.context.get.sid, startEvent.context.get.content_id, startEvent.edata.eks.client, DtRange(startTimestamp,
                endTimestamp), new CESessionSummary(timeSpent, startTimestamp, endTimestamp, timeDiff, loadTime, noOfInteractEvents,
                interactEventsPerMin, pluginSummary, saveSummary, stageSummary, eventSummary, apiCallCount, sideBarEventCount, menuEventCount));
        }.filter(f => (f.ss.time_spent >= 1)).cache()
    }

    override def postProcess(data: RDD[CESessionSummaryOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { sessionMap =>
            val session = sessionMap.ss;
            val mid = CommonUtil.getMessageId("ME_CE_SESSION_SUMMARY", sessionMap.contentId, "SESSION", sessionMap.dateRange, sessionMap.sid);
            val measures = Map(
                "time_spent" -> session.time_spent,
                "start_time" -> session.start_time,
                "end_time" -> session.end_time,
                "time_diff" -> session.time_diff,
                "load_time" -> session.load_time,
                "interact_events_count" -> session.interact_events_count,
                "interact_events_per_min" -> session.interact_events_per_min,
                "plugin_summary" -> session.plugin_summary,
                "save_summary" -> session.save_summary,
                "stage_summary" -> session.stage_summary,
                "events_summary" -> session.events_summary,
                "api_calls_count" -> session.api_calls_count,
                "sidebar_events_count" -> session.sidebar_events_count,
                "menu_events_count" -> session.menu_events_count);
            MeasuredEvent("ME_CE_SESSION_SUMMARY", System.currentTimeMillis(), sessionMap.dateRange.to, "1.0", mid, sessionMap.uid, Option(sessionMap.contentId), None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentEditorSessionSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", sessionMap.dateRange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(sessionMap.sid), None, None, None, None, None, None, None, None, None, Option(sessionMap.client)),
                MEEdata(measures));
        }
    }

}