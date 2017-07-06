package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.GenieKey
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.GData
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import scala.collection.mutable.ListBuffer
import org.ekstep.analytics.framework.Period
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.conf.AppConf


case class InputEventsGenieSummary(gk: GenieKey, events: Buffer[GenieUsageMetricsSummary]) extends Input with AlgoInput
case class GenieUsageMetricsSummary(gk: GenieKey, pdata: PData, total_ts: Double, total_sessions: Long, avg_ts_session: Double, dt_range: DtRange, syncts: Long, contents: Array[String], device_ids: Array[String]) extends AlgoOutput;

object GenieUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, InputEventsGenieSummary, GenieUsageMetricsSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.GenieUsageSummaryModel"
    override def name: String = "GenieUsageSummaryModel"
    
    private def _computeMetrics(events: Buffer[GenieUsageMetricsSummary], gk: GenieKey): GenieUsageMetricsSummary = {

        val firstEvent = events.sortBy { x => x.dt_range.from }.head;
        val lastEvent = events.sortBy { x => x.dt_range.to }.last;
        val gk = firstEvent.gk;

        val date_range = DtRange(firstEvent.dt_range.from, lastEvent.dt_range.to);
        val total_ts = CommonUtil.roundDouble(events.map { x => x.total_ts }.sum, 2);
        val total_sessions = events.size
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
        val contents = events.map { x => x.contents }.reduce((a,b) => a ++ b).distinct;
        val device_ids = events.map { x => x.device_ids }.reduce((a,b) => a ++ b).distinct;
        GenieUsageMetricsSummary(gk, firstEvent.pdata, total_ts, total_sessions, avg_ts_session, date_range, lastEvent.syncts, contents, device_ids);
    }
    
    private def getGenieUsageSummary(event: DerivedEvent, period: Int, pdata: PData, channel: String, tagId: String): GenieUsageMetricsSummary = {

        val gk = GenieKey(period, pdata.id, channel, tagId);
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val total_ts = eksMap.get("timeSpent").get.asInstanceOf[Double];
        val total_sessions = 1;
        val avg_ts_session = total_ts;
        val contents = eksMap.get("content").get.asInstanceOf[List[String]].toArray;
        GenieUsageMetricsSummary(gk, pdata, total_ts, total_sessions, avg_ts_session, event.context.date_range, event.syncts, contents, Array(event.dimensions.did.get));
    }
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[InputEventsGenieSummary] = {
        val configMapping = sc.broadcast(config);
        val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).filter{x=> true==x.active}.map { x => x.tag_id }.collect
        val registeredTags = if (tags.nonEmpty) tags; else Array[String]();

        val sessionEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_GENIE_LAUNCH_SUMMARY")));

        val normalizeEvents = sessionEvents.map { event =>

            var list: ListBuffer[GenieUsageMetricsSummary] = ListBuffer[GenieUsageMetricsSummary]();
            val period = CommonUtil.getPeriod(event.context.date_range.to, Period.DAY);
            // For all
            
            val pdata = CommonUtil.getAppDetails(event)
            val channel = CommonUtil.getChannelId(event)
            
            list += getGenieUsageSummary(event, period, pdata, channel, "all");
            val tags = CommonUtil.getValidTags(event, registeredTags);
            for (tag <- tags) {
                list += getGenieUsageSummary(event, period, pdata, channel, tag);
            }
            list.toArray;
        }.flatMap { x => x.map { x => x } };

        normalizeEvents.map { x => (x.gk, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => InputEventsGenieSummary(x._1, x._2) };
    }
    override def algorithm(data: RDD[InputEventsGenieSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieUsageMetricsSummary] = {
        data.map { x =>
            _computeMetrics(x.events, x.gk);
        }
    }

    override def postProcess(data: RDD[GenieUsageMetricsSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { guMetrics =>
            val mid = CommonUtil.getMessageId("ME_GENIE_USAGE_SUMMARY", guMetrics.gk.tag + guMetrics.gk.period, "DAY", guMetrics.syncts, Option(guMetrics.gk.app_id), Option(guMetrics.gk.channel));
            val measures = Map(
                "total_ts" -> guMetrics.total_ts,
                "total_sessions" -> guMetrics.total_sessions,
                "avg_ts_session" -> guMetrics.avg_ts_session,
                "contents" -> guMetrics.contents,
                "device_ids" -> guMetrics.device_ids)

            MeasuredEvent("ME_GENIE_USAGE_SUMMARY", System.currentTimeMillis(), guMetrics.syncts, "1.0", mid, "", Option(guMetrics.gk.channel), None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "GenieUsageSummaryModel").asInstanceOf[String])), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], guMetrics.dt_range),
                Dimensions(None, None, None, None, None, None, Option(guMetrics.pdata), None, None, None, Option(guMetrics.gk.tag), Option(guMetrics.gk.period)),
                MEEdata(measures));
        }
    }
}