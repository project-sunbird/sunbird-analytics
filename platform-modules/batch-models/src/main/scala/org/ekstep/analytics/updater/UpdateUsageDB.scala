/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.BloomFilterUtil
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.ekstep.analytics.connector.InfluxDB._
import com.datastax.spark.connector._



case class MEUsageSummaryFact(d_period: Int, d_user_id: String, d_content_id: String, d_tag: String, d_app_id: String, d_channel: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_users_count: Long, m_total_content_count: Long, m_total_devices_count : Long, m_user_ids: Array[Byte], m_content_ids: Array[Byte], m_device_ids: Array[Byte], updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with CassandraTable;
case class MESummaryIndex(d_period: Int, d_user_id: String, d_content_id: String, d_tag: String, d_app_id: String, d_channel: String) extends Output;
case class MEUsageSummaryFact_T(d_period: Int, d_user_id: String, d_content_id: String, d_tag: String, d_app_id: String, d_channel: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime,
                                m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double,
                                m_user_ids: List[String], m_content_ids: List[String], m_device_ids: List[String]) extends AlgoOutput

object UpdateUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, MEUsageSummaryFact, MESummaryIndex] with Serializable with IInfluxDBUpdater {
    
    val className = "org.ekstep.analytics.updater.UpdateMEUsageDB"
    override def name: String = "UpdateMEUsageDB"
    val USAGE_SUMMARY_METRICS = "usage_summary_metrics"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_USAGE_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MEUsageSummaryFact] = {

        val meSummary = data.map { x =>

            val period = x.dimensions.period.get;
            val contentId = x.dimensions.content_id.get;
            val tag = x.dimensions.tag.get;
            val appId = CommonUtil.getAppDetails(x).id
            val channel = CommonUtil.getChannelId(x)
            val user_id = x.dimensions.uid

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val publish_date = new DateTime(x.context.date_range.from)
            val total_ts = eksMap.get("total_ts").get.asInstanceOf[Double]
            val total_sessions = eksMap.get("total_sessions").get.asInstanceOf[Int]
            val avg_ts_session = eksMap.get("avg_ts_session").get.asInstanceOf[Double]
            val total_interactions = eksMap.get("total_interactions").get.asInstanceOf[Int]
            val avg_interactions_min = eksMap.get("avg_interactions_min").get.asInstanceOf[Double]
            val user_ids = eksMap.get("user_ids").getOrElse(List("")).asInstanceOf[List[String]];
            val content_ids = eksMap.get("content_ids").getOrElse(List("")).asInstanceOf[List[String]];
            val device_ids = eksMap.get("device_ids").getOrElse(List("")).asInstanceOf[List[String]];

            MEUsageSummaryFact_T(period, user_id.get, contentId, tag, appId, channel, publish_date, new DateTime(x.syncts), new DateTime(x.context.date_range.to), total_ts, total_sessions, avg_ts_session,
                total_interactions, avg_interactions_min, user_ids, content_ids, device_ids);
        }.cache();

        // Roll up summaries
        rollup(meSummary, DAY).union(rollup(meSummary, WEEK)).union(rollup(meSummary, MONTH)).union(rollup(meSummary, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[MEUsageSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MESummaryIndex] = {
        // Update the database
        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT)
        saveToInfluxDB(data)
        data.map { x => MESummaryIndex(x.d_period, x.d_user_id, x.d_content_id, x.d_tag, x.d_app_id, x.d_channel) };
    }

    /**
     * Rollup daily summaries by period. The period summaries are joined with the previous entries in the database and then reduced to produce new summaries.
     */
    private def rollup(data: RDD[MEUsageSummaryFact_T], period: Period): RDD[MEUsageSummaryFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.m_last_gen_date.getMillis, period);
            (MESummaryIndex(d_period, x.d_user_id, x.d_content_id, x.d_tag, x.d_app_id, x.d_channel), MEUsageSummaryFact_T(d_period, x.d_user_id, x.d_content_id, x.d_tag, x.d_app_id, x.d_channel, x.m_publish_date, x.m_last_sync_date, x.m_last_gen_date, x.m_total_ts, x.m_total_sessions, x.m_avg_ts_session, x.m_total_interactions, x.m_avg_interactions_min, x.m_user_ids, x.m_content_ids, x.m_device_ids));
        }.reduceByKey(reduceCUS);
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).on(SomeColumns("d_period", "d_user_id", "d_content_id", "d_tag", "d_app_id", "d_channel"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(MEUsageSummaryFact(index.d_period, index.d_user_id, index.d_content_id, index.d_tag, index.d_app_id, index.d_channel, newSumm.m_publish_date, newSumm.m_last_sync_date, newSumm.m_last_gen_date, 0.0, 0, 0.0, 0, 0.0, 0, 0, 0, BloomFilterUtil.getDefaultBytes(period), BloomFilterUtil.getDefaultBytes(period), BloomFilterUtil.getDefaultBytes(period)));
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduce(fact1: MEUsageSummaryFact, fact2: MEUsageSummaryFact_T, period: Period): MEUsageSummaryFact = {
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2);
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2);
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val publish_date = if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date else fact1.m_publish_date;
        val sync_date = if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date else fact1.m_last_sync_date;
        
        val bf_device_ids = BloomFilterUtil.deserialize(period, fact1.m_device_ids);
        val didCount = BloomFilterUtil.countMissingValues(bf_device_ids, fact2.m_device_ids);
        val total_devices = didCount + fact1.m_total_devices_count;
        
        val bf_user_ids = BloomFilterUtil.deserialize(period, fact1.m_user_ids);
        val userCount = BloomFilterUtil.countMissingValues(bf_user_ids, fact2.m_user_ids);
        val total_user = userCount + fact1.m_total_users_count;
        
        val bf_content_ids = BloomFilterUtil.deserialize(period, fact1.m_content_ids);
        val contentCount = BloomFilterUtil.countMissingValues(bf_content_ids, fact2.m_content_ids);
        val total_content = contentCount + fact1.m_total_content_count
        
        val device_ids = BloomFilterUtil.serialize(bf_device_ids);
        val user_ids = BloomFilterUtil.serialize(bf_user_ids);
        val content_ids = BloomFilterUtil.serialize(bf_content_ids);

        MEUsageSummaryFact(fact1.d_period, fact1.d_user_id, fact1.d_content_id, fact1.d_tag, fact1.d_app_id, fact1.d_channel, publish_date, sync_date, fact2.m_last_gen_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, total_user, total_content, total_devices, user_ids, content_ids, device_ids);
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduceCUS(fact1: MEUsageSummaryFact_T, fact2: MEUsageSummaryFact_T): MEUsageSummaryFact_T = {
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2);
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2);
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val publish_date = if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date else fact1.m_publish_date;
        val sync_date = if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date else fact1.m_last_sync_date;
        val device_ids = (fact2.m_device_ids ++ fact1.m_device_ids).distinct
        val user_ids = (fact2.m_user_ids ++ fact1.m_user_ids).distinct
        val content_ids = (fact2.m_content_ids ++ fact1.m_content_ids).distinct
        

        MEUsageSummaryFact_T(fact1.d_period, fact1.d_user_id, fact1.d_content_id, fact1.d_tag, fact1.d_app_id, fact1.d_channel, publish_date, sync_date, fact2.m_last_gen_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, user_ids, content_ids, device_ids);
    }
    
    private def saveToInfluxDB(data: RDD[MEUsageSummaryFact])(implicit sc: SparkContext) {
        val metrics = data.filter { x => x.d_period != 0 } map { x =>
            val fields = (CommonUtil.caseClassToMapWithDateConversion(x) - ("d_period", "d_user_id", "d_content_id", "d_tag", "d_app_id", "d_channel", "updated_date", "m_publish_date", "m_last_sync_date", "m_last_gen_date", "m_content_ids", "m_user_ids", "m_device_ids")).map(f => (f._1, f._2.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef]));
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("period" -> time._2, "user_id" -> x.d_user_id, "content_id" -> x.d_content_id, "tag" -> x.d_tag, "app_id" -> x.d_app_id, "channel" -> x.d_channel), Map("total_ts" -> fields.get("m_total_ts").get, "total_sessions" -> fields.get("m_total_sessions").get, "avg_ts_session" -> fields.get("m_avg_ts_session").get, "total_interactions" -> fields.get("m_total_interactions").get, "avg_interactions_min" -> fields.get("m_avg_interactions_min").get, "total_users_count" -> fields.get("m_total_users_count").get, "total_content_count" -> fields.get("m_total_content_count").get, "total_devices_count" -> fields.get("m_total_devices_count").get), time._1);
        };
        val usageEvents  = getDenormalizedData("content", data.map { x => x.d_content_id })
        val userEvents  = getDenormalizedData("User", data.map { x => x.d_user_id })
        metrics.denormalize("content_id", "content_name", usageEvents).denormalize("user_id", "user_name", userEvents).saveToInflux(USAGE_SUMMARY_METRICS);
    }
}