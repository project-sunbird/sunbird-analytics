package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JobLogger
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime

case class ContentUsageMetrics(content_id: Option[String] = None, content_ver: Option[String] = None, tag: Option[String] = None, total_ts: Double, total_sessions: Long, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, dt_range: DtRange, syncts: Long, period: Int) extends AlgoOutput;
case class RegisteredTag(tag_id: String)
case class InputEvents(day: Int, events: Buffer[DerivedEvent]) extends Input with AlgoInput

object ContentUsageSummary extends IBatchModelTemplate[DerivedEvent, InputEvents, ContentUsageMetrics, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ContentUsageSummary"
    override def name: String = "ContentUsageSummarizer"

    private def _getTagList(event: DerivedEvent, tags: Array[String]): List[String] = {
        val tagList = event.tags.getOrElse(List()).asInstanceOf[List[Map[String, List[String]]]]
        val genieTagFilter = if (tagList.nonEmpty) tagList.filter(f => f.contains("genie")) else List()
        val tempList = if (genieTagFilter.nonEmpty) genieTagFilter.filter(f => f.contains("genie")).last.get("genie").get; else List();
        tempList.filter { x => tags.contains(x) };
    }

    private def _getregisteredTags(config: Map[String, AnyRef])(implicit sc: SparkContext): Array[String] = {
        val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).select("tag_id").where("active = ?", true).map { x => x.tag_id }.collect
        if (tags.nonEmpty) tags; else config.getOrElse("tagList", Array()).asInstanceOf[Array[String]];
    }

    private def _computeMetrics(events: Buffer[DerivedEvent], period: Int, content_id: Option[String] = None, tag: Option[String] = None): ContentUsageMetrics = {

        val firstEvent = events.sortBy { x => x.context.date_range.from }.head;
        val lastEvent = events.sortBy { x => x.context.date_range.to }.last;
        val ver = if (content_id != None) Option(firstEvent.dimensions.gdata.get.ver); else None
        val date_range = DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to);
        val total_ts = CommonUtil.roundDouble(events.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double]) }.sum, 2);
        val total_sessions = events.size
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
        val total_interactions = events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int] }.sum.toLong
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        ContentUsageMetrics(content_id, ver, tag, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, date_range, lastEvent.syncts, period);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[InputEvents] = {
        val configMapping = sc.broadcast(config);

        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        filteredEvents.map { x => (CommonUtil.getPeriod(x.context.date_range.to, Period.DAY), Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => InputEvents(x._1, x._2) };
    }

    override def algorithm(data: RDD[InputEvents], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentUsageMetrics] = {

        val inputs = data.cache()

        // all summary
        val allSummary = inputs.map { x => _computeMetrics(x.events, x.day); }

        // per content summary
        val contentSummary = inputs.map { x =>
            x.events.groupBy(f => f.dimensions.gdata.get.id).map { f => _computeMetrics(f._2, x.day, Option(f._1)); };
        }.flatMap { x => x }

        val registeredTags = _getregisteredTags(config)(sc)

        // (per tag) & (per tag,per content) summary
        val tagSummary_tagContentSummary = inputs.map { x =>

            val period = x.day
            val tagEvent = x.events.map { x => (_getTagList(x, registeredTags), Buffer(x)); }
                .map { x => x._1.map { f => (f, x._2) }; }.flatMap(f => f).groupBy(f => f._1)

            // per tag Summary
            val tagSum = tagEvent.map { f =>
                f._2.map { x => x._2 }.map { x => _computeMetrics(x, period, None, Option(f._1)); }
            }.flatMap { x => x }

            // per tag per content Summary
            val tagContentSum = tagEvent.map { tagEvents =>
                val tag = tagEvents._1
                tagEvents._2.map { x => x._2 }.map { x =>
                    x.groupBy(f => f.dimensions.gdata.get.id).map { f => _computeMetrics(f._2, period, Option(f._1), Option(tagEvents._1)); }
                }.flatMap { x => x };
            }.flatMap { x => x };

            (tagSum ++ tagContentSum);
        }.flatMap { x => x }

        inputs.unpersist(true)
        val rdd1 = contentSummary.union(allSummary)
        rdd1.union(tagSummary_tagContentSummary);
    }

    override def postProcess(data: RDD[ContentUsageMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { cuMetrics =>
            val mid = CommonUtil.getMessageId("ME_CONTENT_USAGE_SUMMARY", "", config.getOrElse("granularity", "DAY").asInstanceOf[String], cuMetrics.dt_range, cuMetrics.content_id.getOrElse(""), cuMetrics.tag.getOrElse(""), cuMetrics.period);
            val measures = Map(
                "total_ts" -> cuMetrics.total_ts,
                "total_sessions" -> cuMetrics.total_sessions,
                "avg_ts" -> cuMetrics.avg_ts_session,
                "total_interactions" -> cuMetrics.total_interactions,
                "avg_interactions_min" -> cuMetrics.avg_interactions_min)

            val gdata = if (cuMetrics.content_id != None) Option(new GData(cuMetrics.content_id.get, cuMetrics.content_ver.get)) else None

            MeasuredEvent("ME_CONTENT_USAGE_SUMMARY", System.currentTimeMillis(), cuMetrics.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentUsageSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], cuMetrics.dt_range),
                Dimensions(None, None, gdata, None, None, None, None, None, None, cuMetrics.tag),
                MEEdata(measures));
        }
    }
}