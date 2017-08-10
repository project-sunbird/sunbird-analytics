/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.model

import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Period
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants

import com.datastax.spark.connector._


case class UsageSummaryIndex(period: Int, app_id: String, channel: String, user_id: String, content_id: String, tag: String);
case class UsageSummary(ck: UsageSummaryIndex, total_ts: Double, total_sessions: Long, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, total_users_count: Long, total_content_count: Long, total_devices_count : Long, user_ids: Array[String], content_ids: Array[String], device_ids: Array[String], uid: String, dt_range: DtRange, syncts: Long, gdata: Option[GData] = None, pdata: PData) extends AlgoOutput;
case class InputEventsSummary(ck: UsageSummaryIndex, events: Buffer[UsageSummary]) extends Input with AlgoInput

object UsageSummaryModel extends IBatchModelTemplate[DerivedEvent, InputEventsSummary, UsageSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.UsageSummaryModel"
    override def name: String = "UsageSummaryModel"

    private def _computeMetrics(events: Buffer[UsageSummary], ck: UsageSummaryIndex): UsageSummary = {
        val firstEvent = events.sortBy { x => x.dt_range.from }.head;
        val lastEvent = events.sortBy { x => x.dt_range.to }.last;
        val ck = firstEvent.ck;

        val gdata = if (StringUtils.equals(ck.content_id, "all")) None else Option(new GData(ck.content_id, firstEvent.gdata.get.ver));

        val date_range = DtRange(firstEvent.dt_range.from, lastEvent.dt_range.to);
        val total_ts = CommonUtil.roundDouble(events.map { x => x.total_ts }.sum, 2);
        val total_sessions = events.size
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
        val total_interactions = events.map { x => x.total_interactions }.sum;
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val user_ids = events.map { x => x.user_ids }.reduce((a, b) => a ++ b).distinct;
        val content_ids = events.map { x => x.content_ids }.reduce((a, b) => a ++ b).distinct;
        val device_ids = events.map { x => x.device_ids }.reduce((a, b) => a ++ b).distinct;
        UsageSummary(ck, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, user_ids.size, content_ids.size, device_ids.size, user_ids, content_ids, device_ids, ck.user_id, date_range, lastEvent.syncts, gdata, firstEvent.pdata);
    }

    private def getUsageSummary(event: DerivedEvent, period: Int, pdata: PData, channel: String, userId: String, contentId: String, tagId: String): UsageSummary = {

        val ck = UsageSummaryIndex(period, pdata.id, channel, userId, contentId, tagId)
        val gdata = event.dimensions.gdata.get
        val total_ts = event.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Number].doubleValue()
        val total_sessions = 1
        val avg_ts_session = total_ts
        val total_interactions = event.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Number].longValue()
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val content_ids = if("all".equals(contentId)) Array(event.dimensions.gdata.get.id) else Array[String]() 
        val user_ids = if("all".equals(userId)) Array(event.uid) else Array[String]()

        UsageSummary(ck, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, 0, 0, 0, user_ids, content_ids, Array(event.dimensions.did.get), event.uid, event.context.date_range, event.syncts, Option(gdata), pdata);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[InputEventsSummary] = {
        // TODO: Need to remove logic to collect tag 
        val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).filter { x => x.active }.map { x => x.tag_id }.collect()
        val registeredTags = if (tags.nonEmpty) tags; else Array[String]();
        
        val sessionEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
                
        val normalizeEvents = sessionEvents.map { event =>

            var list: ListBuffer[UsageSummary] = ListBuffer[UsageSummary]();
            val period = CommonUtil.getPeriod(event.context.date_range.to, Period.DAY);
            // For all
            val pdata = CommonUtil.getAppDetails(event)
            val channel = CommonUtil.getChannelId(event)

            list += getUsageSummary(event, period, pdata, channel, "all", "all", "all")
            list += getUsageSummary(event, period, pdata, channel, event.uid, "all", "all")
            list += getUsageSummary(event, period, pdata, channel, "all", event.dimensions.gdata.get.id, "all")
            list += getUsageSummary(event, period, pdata, channel, event.uid, event.dimensions.gdata.get.id, "all")
            val tags = CommonUtil.getValidTags(event, registeredTags); 
            for (tag <- tags) {
                list += getUsageSummary(event, period, pdata, channel, event.uid, "all", tag);
                list += getUsageSummary(event, period, pdata, channel, "all", event.dimensions.gdata.get.id, tag);
                list += getUsageSummary(event, period, pdata, channel, "all", "all", tag);
            }
            list.toArray;
        }.flatMap { x => x.map { x => x } };

        normalizeEvents.map { x => (x.ck, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => InputEventsSummary(x._1, x._2) };
    }

    override def algorithm(data: RDD[InputEventsSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[UsageSummary] = {
        data.map { x =>
            _computeMetrics(x.events, x.ck);
        }
    }

    override def postProcess(data: RDD[UsageSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {        
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { cuMetrics =>
            val mid = CommonUtil.getMessageId("ME_USAGE_SUMMARY", cuMetrics.ck.user_id + cuMetrics.ck.content_id + cuMetrics.ck.tag + cuMetrics.ck.period, "DAY", cuMetrics.syncts, Option(cuMetrics.ck.app_id), Option(cuMetrics.ck.channel));
            
            val measures = Map(
                "total_ts" -> cuMetrics.total_ts,
                "total_sessions" -> cuMetrics.total_sessions,
                "avg_ts_session" -> cuMetrics.avg_ts_session,
                "total_interactions" -> cuMetrics.total_interactions,
                "avg_interactions_min" -> cuMetrics.avg_interactions_min,
                "total_users_count" -> cuMetrics.total_users_count,
                "total_content_count" -> cuMetrics.total_content_count,
                "total_devices_count" -> cuMetrics.total_devices_count,
                "user_ids" -> cuMetrics.user_ids,
                "content_ids" -> cuMetrics.content_ids,
                "device_ids" -> cuMetrics.device_ids)

            MeasuredEvent("ME_USAGE_SUMMARY", System.currentTimeMillis(), cuMetrics.syncts, meEventVersion, mid, "", cuMetrics.ck.channel, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "ME.UsageSummary").asInstanceOf[String])), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], cuMetrics.dt_range),
                Dimensions(Option(cuMetrics.uid), None, None, None, None, None, Option(cuMetrics.pdata), None, None, None, Option(cuMetrics.ck.tag), Option(cuMetrics.ck.period), Option(cuMetrics.ck.content_id)),
                MEEdata(measures));
        }
    }
}