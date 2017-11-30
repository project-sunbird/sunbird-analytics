/**
 * @author Sowmya Dixit
 */
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.creation.model.CreationEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework._
import org.ekstep.analytics.creation.model.CreationEData
import org.ekstep.analytics.creation.model.CreationEks
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.CreationEventUtil
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.creation.model.CreationPData

/**
 * Case Classes for the data product
 */
case class PortalSessionInput(channel: String, sid: String, filteredEvents: Buffer[V3Event]) extends AlgoInput
case class PageSummary(id: String, `type`: String, env: String, time_spent: Double, visit_count: Long)
case class EnvSummary(env: String, time_spent: Double, count: Long)
case class PortalSessionOutput(sid: String, uid: String, pdata: PData, channel: String, syncTs: Long, anonymousUser: Boolean, dtRange: DtRange,
                               start_time: Long, end_time: Long, time_spent: Double, time_diff: Double, page_views_count: Long,
                               first_visit: Boolean, ce_visits: Long, interact_events_count: Long, interact_events_per_min: Double,
                               env_summary: Option[Iterable[EnvSummary]], events_summary: Option[Iterable[EventSummary]],
                               page_summary: Option[Iterable[PageSummary]], etags: Option[ETags]) extends AlgoOutput

/**
 * @dataproduct
 * @Summarizer
 *
 * AppSessionSummaryModel
 *
 * Functionality
 * 1. Generate app specific session summary events. This would be used to compute app usage metrics.
 * Events used - BE_OBJECT_LIFECYCLE, CP_SESSION_START, CE_START, CE_END, CP_INTERACT & CP_IMPRESSION
 */
object AppSessionSummaryModel extends IBatchModelTemplate[V3Event, PortalSessionInput, PortalSessionOutput, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.AppSessionSummaryModel"
    override def name: String = "AppSessionSummaryModel"

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalSessionInput] = {
        JobLogger.log("Filtering Events of BE_OBJECT_LIFECYCLE, CP_SESSION_START, CE_START, CE_END, CP_INTERACT, CP_IMPRESSION")
        val filteredData = DataFilter.filter(data, Array(Filter("eventId", "IN", Option(List("AUDIT", "START", "INTERACT", "IMPRESSION", "END"))), Filter("context.pdata", "ISNOTEMPTY", None))).filter { x => ((x.context.pdata.get.id.contains(Constants.PORTAL_ENV)) || ((x.context.env.equals(Constants.EDITOR_ENV)) && ("IMPRESSION".equals(x.eid) || "END".equals(x.eid)))) };
        filteredData.map { event =>
            val channel = CommonUtil.getChannelId(event)
            ((channel, event.context.sid.get), Buffer(event))
        }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events => events.sortBy { x => x.ets } }
            .map { x => PortalSessionInput(x._1._1, x._1._2, x._2) }
    }

    override def algorithm(data: RDD[PortalSessionInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalSessionOutput] = {

        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];

        data.map { x =>
            val events = x.filteredEvents
            val firstEvent = events.head;
            val lastEvent = events.last;
            val telemetryVer = firstEvent.ver;
            val startTimestamp = firstEvent.ets;
            val endTimestamp = lastEvent.ets;
            val pdata = CommonUtil.getAppDetails(firstEvent)
            val channelId = x.channel
            val uid = if (lastEvent.actor.id.isEmpty()) "" else lastEvent.actor.id
            val isAnonymous = if (uid.isEmpty()) true else false
            val lifeCycleEvent = events.filter { x => "AUDIT".equals(x.eid) && "User".equals(x.actor.`type`) && "Create".equals(x.edata.state) }
            val firstVisit = if (lifeCycleEvent.size > 0) true else false
            val timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get, 2);

            var tmpLastEvent: V3Event = null;
            val eventsWithTs = events.map { x =>
                if (tmpLastEvent == null) tmpLastEvent = x;
                val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
                tmpLastEvent = x;
                (x, if (ts > idleTime) 0 else ts)
            }
            var lastEventTs: Long = eventsWithTs.last._1.ets;
            var tempEvents = Buffer[(V3Event, Double)]();
            var eventsBuffer: Buffer[(V3Event, Double)] = Buffer();
            eventsWithTs.foreach { f =>
                f._1.eid match {
                    case "IMPRESSION" =>
                        if (tempEvents.isEmpty) {
                            tempEvents += f
                        } else {
                            val ts = tempEvents.map { x => x._2 }.sum
                            val tuple = (tempEvents.head._1, ts)
                            eventsBuffer += tuple
                            tempEvents = Buffer[(V3Event, Double)]();
                            tempEvents += f
                        }
                    case _ =>
                        if (lastEventTs == f._1.ets && !tempEvents.isEmpty) {
                            val ts = tempEvents.map { x => x._2 }.sum
                            val tuple = (tempEvents.head._1, ts)
                            eventsBuffer += tuple
                        }
                        tempEvents += f
                }
            }
            val timeSpent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);
            val impressionEvents = events.filter { x => "IMPRESSION".equals(x.eid) }
            val pageViewsCount = impressionEvents.size.toLong
            val ceVisits = events.filter { x => ("IMPRESSION".equals(x.eid) && Constants.EDITOR_ENV.equals(x.context.env)) }.size.toLong
            val interactEventsCount = events.filter { x => ("INTERACT".equals(x.eid) && x.context.pdata.get.id.contains(Constants.PORTAL_ENV)) }.size.toLong
            val interactEventsPerMin: Double = if (interactEventsCount == 0 || timeSpent == 0) 0d
            else if (timeSpent < 60.0) interactEventsCount.toDouble
            else BigDecimal(interactEventsCount / (timeSpent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;

            val eventSummaries = events.groupBy { x => x.eid }.map(f => EventSummary(f._1, f._2.length));

//            val impressionCEEvents = eventsBuffer.filter { x => (("IMPRESSION".equals(x._1.eid) && Constants.PORTAL_ENV.equals(x._1.context.pdata.get.id)) || ("START".equals(x._1.eid) && Constants.EDITOR_ENV.equals(x._1.context.env))) }.map { f =>
//                if ("START".equals(f._1.eid)) {
//                    val edataString = JSONUtils.serialize(Map("env" -> "content-editor", "type" -> "", "pageid" -> "ce"))
//                    val edata = JSONUtils.deserialize[V3EData](edataString)
//                    (new V3Event("IMPRESSION", f._1.ets, f._1.`@timestamp`, f._1.ver, f._1.mid, f._1.actor, f._1.context, f._1.`object`, edata), f._2)
////                    (CreationEvent("CP_IMPRESSION", f._1.ets, f._1.`@timestamp`, f._1.ver, f._1.mid, f._1.channel, f._1.pdata, f._1.cdata, f._1.uid, f._1.context, f._1.rid, new CreationEData(eks), f._1.tags), f._2)
//                } else f;
//            }.map(x => (x._1.edata.pageid, x))

            val impressionEventsWithTs = eventsBuffer.filter { x => "IMPRESSION".equals(x._1.eid)}.map(x => (x._1.edata.pageid, x))
            val pageSummaries = if (impressionEventsWithTs.length > 0) {
                impressionEventsWithTs.groupBy(f => f._1).map { f =>
                    val id = f._1
                    val firstEvent = f._2(0)._2._1
                    val `type` = firstEvent.edata.`type`
                    val env = firstEvent.context.env
                    val timeSpent = CommonUtil.roundDouble(f._2.map(x => x._2._2).sum, 2)
                    val visitCount = f._2.length.toLong
                    PageSummary(id, `type`, env, timeSpent, visitCount)
                }
            } else Iterable[PageSummary]();

            val envSummaries = if (pageSummaries.size > 0) {
                pageSummaries.groupBy { x => x.env }.map { f =>
                    val timeSpent = CommonUtil.roundDouble(f._2.map(x => x.time_spent).sum, 2)
                    val count = f._2.map(x => x.visit_count).max;
                    EnvSummary(f._1, timeSpent, count)
                }
            } else Iterable[EnvSummary]();

            PortalSessionOutput(x.sid, uid, pdata, channelId, CommonUtil.getEventSyncTS(lastEvent), isAnonymous, DtRange(startTimestamp, endTimestamp), startTimestamp, endTimestamp, timeSpent, timeDiff, pageViewsCount, firstVisit, ceVisits, interactEventsCount, interactEventsPerMin, Option(envSummaries), Option(eventSummaries), Option(pageSummaries), Option(CommonUtil.getETags(firstEvent)))
        }.filter(f => (f.time_spent >= 1))
    }

    override def postProcess(data: RDD[PortalSessionOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { session =>
            val mid = CommonUtil.getMessageId("ME_APP_SESSION_SUMMARY", session.uid, "SESSION", session.dtRange, "NA", Option(session.pdata.id), Option(session.channel));
            val measures = Map(
                "start_time" -> session.start_time,
                "end_time" -> session.end_time,
                "time_diff" -> session.time_diff,
                "time_spent" -> session.time_spent,
                "interact_events_count" -> session.interact_events_count,
                "interact_events_per_min" -> session.interact_events_per_min,
                "first_visit" -> session.first_visit,
                "ce_visits" -> session.ce_visits,
                "page_views_count" -> session.page_views_count,
                "env_summary" -> session.env_summary,
                "events_summary" -> session.events_summary,
                "page_summary" -> session.page_summary);
            MeasuredEvent("ME_APP_SESSION_SUMMARY", System.currentTimeMillis(), session.syncTs, meEventVersion, mid, session.uid, session.channel, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "AppSessionSummarizer").asInstanceOf[String])), None, "SESSION", session.dtRange),
                Dimensions(None, None, None, None, None, None, Option(PData(session.pdata.id, session.pdata.ver)), None, None, Option(session.anonymousUser), None, None, None, None, None, Option(session.sid)),
                MEEdata(measures), session.etags);
        }
    }
}