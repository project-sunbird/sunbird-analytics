package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Item
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework.Content
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.GData
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.LearnerProfile
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.util.DerivedEvent
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.ETags

case class StageSummary(uid: String, groupUser: Boolean, anonymousUser: Boolean, sid: String, syncts: Long, gdata: GData, did: String, etags: Option[ETags] = Option(ETags(None, None, None)), dt_range: DtRange, stageId: String, timeSpent: Double, visitCount: Long, pdata: PData, channel: String) extends AlgoOutput

object StageSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, StageSummary, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.StageSummaryModel"
    override def name(): String = "StageSummaryModel";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[StageSummary] = {
        data.map { event =>
            val pdata = CommonUtil.getAppDetails(event)
            val channel = CommonUtil.getChannelId(event)
            val screenSummaries = event.edata.eks.screenSummary;
            if (null != screenSummaries && screenSummaries.size > 0) {
                screenSummaries.map { x =>
                    val ss = JSONUtils.deserialize[ScreenSummary](JSONUtils.serialize(x));
                    StageSummary(event.uid, event.dimensions.group_user, event.dimensions.anonymous_user, event.mid, event.syncts, event.dimensions.gdata, event.dimensions.did, event.etags, event.context.date_range, ss.id, ss.timeSpent, ss.visitCount, pdata, channel)
                }
            } else {
                Array[StageSummary]();
            }
        }.filter { x => !x.isEmpty }.flatMap { x => x.map { x => x } }
    }

    override def postProcess(data: RDD[StageSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_STAGE_SUMMARY", summary.stageId + summary.sid, summary.uid, summary.dt_range.to, Option(summary.pdata.id), Option(summary.channel));
            val measures = Map(
                "stageId" -> summary.stageId,
                "timeSpent" -> summary.timeSpent,
                "visitCount" -> summary.visitCount);
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "ScreenSummary").asInstanceOf[String]));
            MeasuredEvent("ME_STAGE_SUMMARY", System.currentTimeMillis(), summary.syncts, meEventVersion, mid, summary.uid, summary.channel, Option(summary.gdata.id), None,
                Context(pdata, None, "EVENT", summary.dt_range),
                Dimensions(None, Option(summary.did), Option(summary.gdata), None, None, None, Option(summary.pdata), None, Option(summary.groupUser), Option(summary.anonymousUser), None, None, None, Option(summary.sid)), MEEdata(measures), summary.etags);
        };
    }
}