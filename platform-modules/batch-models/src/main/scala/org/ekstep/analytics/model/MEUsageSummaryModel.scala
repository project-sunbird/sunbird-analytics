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
import org.ekstep.analytics.util.DerivedEvent

import com.datastax.spark.connector.toSparkContextFunctions



case class MEKey(period: Int, app_id: String, channel: String, user_id: String, content_id: String, tag: String);
case class MEUsageSummary(ck: MEKey, total_ts: Double, total_sessions: Long, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, user_count: Long, device_ids: Array[String], uid: String, dt_range: DtRange, syncts: Long, gdata: Option[GData] = None, pdata: PData) extends AlgoOutput;
case class InputEventsMESummary(ck: MEKey, events: Buffer[MEUsageSummary]) extends Input with AlgoInput

object MEUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, InputEventsMESummary, MEUsageSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.MEUsageSummaryModel"
    override def name: String = "MEUsageSummaryModel"

    private def _computeMetrics(events: Buffer[MEUsageSummary], ck: MEKey): MEUsageSummary = {
        
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
        val user_count = events.map { f => f.uid }.distinct.size
        val device_ids = events.map { x => x.device_ids }.reduce((a, b) => a ++ b).distinct;
        MEUsageSummary(ck, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, user_count, device_ids, ck.user_id, date_range, lastEvent.syncts, gdata, firstEvent.pdata);
    }

    private def getMEUsageSummary(event: DerivedEvent, period: Int, pdata: PData, channel: String, userId: String, contentId: String, tagId: String): MEUsageSummary = {

        val ck = MEKey(period, pdata.id, channel, userId, contentId, tagId)
        val gdata = event.dimensions.gdata
        val total_ts = event.edata.eks.timeSpent
        val total_sessions = 1
        val avg_ts_session = total_ts
        val total_interactions = event.edata.eks.noOfInteractEvents
        val user_count = 1
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        MEUsageSummary(ck, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, user_count, Array(event.dimensions.did), event.uid, event.context.date_range, event.syncts, Option(gdata), pdata);
    }

    private def _getValidTags(event: DerivedEvent, registeredTags: Array[String]): Array[String] = {
        val appTag = event.etags.get.app
        val genieTagFilter = if (appTag.isDefined) appTag.get else List()
        genieTagFilter.filter { x => registeredTags.contains(x) }.toArray;
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[InputEventsMESummary] = {
        val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).filter { x => true == x.active }.map { x => x.tag_id }.collect
        val registeredTags = if (tags.nonEmpty) tags; else Array[String]();

        val sessionEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));

        val normalizeEvents = sessionEvents.map { event =>

            var list: ListBuffer[MEUsageSummary] = ListBuffer[MEUsageSummary]();
            val period = CommonUtil.getPeriod(event.context.date_range.to, Period.DAY);
            // For all
            val pdata = CommonUtil.getAppDetails(event)
            val channel = CommonUtil.getChannelId(event)

            list += getMEUsageSummary(event, period, pdata, channel, "all", "all", "all")
            list += getMEUsageSummary(event, period, pdata, channel, event.uid, "all", "all")
            list += getMEUsageSummary(event, period, pdata, channel, "all", event.dimensions.gdata.id, "all")
            list += getMEUsageSummary(event, period, pdata, channel, event.uid, event.dimensions.gdata.id, "all")
            val tags = _getValidTags(event, registeredTags);
            for (tag <- tags) {
                list += getMEUsageSummary(event, period, pdata, channel, event.uid, "all", tag);
                list += getMEUsageSummary(event, period, pdata, channel, "all", event.dimensions.gdata.id, tag);
                list += getMEUsageSummary(event, period, pdata, channel, "all", "all", tag);
            }
            list.toArray;
        }.flatMap { x => x.map { x => x } };

        normalizeEvents.map { x => (x.ck, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => InputEventsMESummary(x._1, x._2) };
    }

    override def algorithm(data: RDD[InputEventsMESummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MEUsageSummary] = {
        data.map { x =>
            _computeMetrics(x.events, x.ck);
        }
    }

    override def postProcess(data: RDD[MEUsageSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { cuMetrics =>
            val mid = CommonUtil.getMessageId("ME_USAGE_SUMMARY", cuMetrics.ck.content_id + cuMetrics.ck.tag + cuMetrics.ck.period, "DAY", cuMetrics.syncts, Option(cuMetrics.ck.app_id), Option(cuMetrics.ck.channel));
            val measures = Map(
                "total_ts" -> cuMetrics.total_ts,
                "total_sessions" -> cuMetrics.total_sessions,
                "avg_ts_session" -> cuMetrics.avg_ts_session,
                "total_interactions" -> cuMetrics.total_interactions,
                "avg_interactions_min" -> cuMetrics.avg_interactions_min,
                "user_count" -> cuMetrics.user_count,
                "device_ids" -> cuMetrics.device_ids)

            MeasuredEvent("ME_USAGE_SUMMARY", System.currentTimeMillis(), cuMetrics.syncts, meEventVersion, mid, "", cuMetrics.ck.channel, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "ME.UsageSummary").asInstanceOf[String])), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], cuMetrics.dt_range),
                Dimensions(Option(cuMetrics.uid), None, cuMetrics.gdata, None, None, None, Option(cuMetrics.pdata), None, None, None, Option(cuMetrics.ck.tag), Option(cuMetrics.ck.period), Option(cuMetrics.ck.content_id)),
                MEEdata(measures));
        }
    }

}