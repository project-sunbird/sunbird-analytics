package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Period._
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.joda.time.LocalDate
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.util.BloomFilterUtil
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.util.ContentSummaryIndex
import org.ekstep.analytics.framework.conf.AppConf

case class ContentUsageSummaryFact_T(d_period: Int, d_content_id: String, d_tag: String, d_app_id: String, d_channel: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime,
                                     m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double,
                                     m_device_ids: List[String]) extends AlgoOutput

object UpdateContentUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ContentUsageSummaryFact, ContentSummaryIndex] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateContentUsageDB"
    override def name: String = "UpdateContentUsageDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CONTENT_USAGE_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentUsageSummaryFact] = {

        val contentSummary = data.map { x =>

            val period = x.dimensions.period.get;
            val contentId = x.dimensions.content_id.get;
            val tag = x.dimensions.tag.get;
            val appId = CommonUtil.getAppDetails(x).id
            val channel = CommonUtil.getChannelId(x)

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val publish_date = new DateTime(x.context.date_range.from)
            val total_ts = eksMap.get("total_ts").get.asInstanceOf[Double]
            val total_sessions = eksMap.get("total_sessions").get.asInstanceOf[Int]
            val avg_ts_session = eksMap.get("avg_ts_session").get.asInstanceOf[Double]
            val total_interactions = eksMap.get("total_interactions").get.asInstanceOf[Int]
            val avg_interactions_min = eksMap.get("avg_interactions_min").get.asInstanceOf[Double]
            val device_ids = eksMap.get("device_ids").getOrElse(List("")).asInstanceOf[List[String]];

            ContentUsageSummaryFact_T(period, contentId, tag, appId, channel, publish_date, new DateTime(x.syncts), new DateTime(x.context.date_range.to), total_ts, total_sessions, avg_ts_session,
                total_interactions, avg_interactions_min, device_ids);
        }.cache();

        // Roll up summaries
        rollup(contentSummary, DAY).union(rollup(contentSummary, WEEK)).union(rollup(contentSummary, MONTH)).union(rollup(contentSummary, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[ContentUsageSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSummaryIndex] = {
        // Update the database
        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)
        data.map { x => ContentSummaryIndex(x.d_period, x.d_content_id, x.d_tag, x.d_app_id, x.d_channel) };
    }

    /**
     * Rollup daily summaries by period. The period summaries are joined with the previous entries in the database and then reduced to produce new summaries.
     */
    private def rollup(data: RDD[ContentUsageSummaryFact_T], period: Period): RDD[ContentUsageSummaryFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.m_last_gen_date.getMillis, period);
            (ContentSummaryIndex(d_period, x.d_content_id, x.d_tag, x.d_app_id, x.d_channel), ContentUsageSummaryFact_T(d_period, x.d_content_id, x.d_tag, x.d_app_id, x.d_channel, x.m_publish_date, x.m_last_sync_date, x.m_last_gen_date, x.m_total_ts, x.m_total_sessions, x.m_avg_ts_session, x.m_total_interactions, x.m_avg_interactions_min, x.m_device_ids));
        }.reduceByKey(reduceCUS);
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).on(SomeColumns("d_period", "d_content_id", "d_tag", "d_app_id", "d_channel"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(ContentUsageSummaryFact(index.d_period, index.d_content_id, index.d_tag, index.d_app_id, index.d_channel, newSumm.m_publish_date, newSumm.m_last_sync_date, newSumm.m_last_gen_date, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, BloomFilterUtil.getDefaultBytes(period)));
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduce(fact1: ContentUsageSummaryFact, fact2: ContentUsageSummaryFact_T, period: Period): ContentUsageSummaryFact = {
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2);
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2);
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val publish_date = if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date else fact1.m_publish_date;
        val sync_date = if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date else fact1.m_last_sync_date;

        val bf = BloomFilterUtil.deserialize(period, fact1.m_device_ids);
        val didCount = BloomFilterUtil.countMissingValues(bf, fact2.m_device_ids);

        val total_devices = didCount + fact1.m_total_devices;
        val avg_sess_device = CommonUtil.roundDouble(total_sessions.toDouble / total_devices, 2);
        val device_ids = BloomFilterUtil.serialize(bf);

        ContentUsageSummaryFact(fact1.d_period, fact1.d_content_id, fact1.d_tag, fact1.d_app_id, fact1.d_channel, publish_date, sync_date, fact2.m_last_gen_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, total_devices, avg_sess_device, device_ids);
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduceCUS(fact1: ContentUsageSummaryFact_T, fact2: ContentUsageSummaryFact_T): ContentUsageSummaryFact_T = {
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2);
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2);
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val publish_date = if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date else fact1.m_publish_date;
        val sync_date = if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date else fact1.m_last_sync_date;
        val device_ids = (fact2.m_device_ids ++ fact1.m_device_ids).distinct

        ContentUsageSummaryFact_T(fact1.d_period, fact1.d_content_id, fact1.d_tag, fact1.d_app_id, fact1.d_channel, publish_date, sync_date, fact2.m_last_gen_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, device_ids);
    }

}