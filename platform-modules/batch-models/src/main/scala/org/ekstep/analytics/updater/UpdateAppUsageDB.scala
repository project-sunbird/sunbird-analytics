/**
 * @author Sowmya Dixit
 */
package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.CommonUtil._
import java.util.Calendar
import java.text.SimpleDateFormat
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Period._
import com.datastax.spark.connector._
import org.ekstep.analytics.connector.InfluxDB._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.BloomFilterUtil
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.joda.time.DateTime
import org.ekstep.analytics.framework.conf.AppConf

/**
 * Case Classes for the data product
 */
case class PortalUsageSummaryIndex(d_period: Int, d_author_id: String, d_app_id: String, d_channel: String) extends Output
case class PortalUsageSummaryFact(d_period: Int, d_author_id: String, d_app_id: String, d_channel: String, anon_total_sessions: Long, anon_total_ts: Double,
                                  total_sessions: Long, total_ts: Double, ce_total_sessions: Long, ce_percent_sessions: Double,
                                  total_pageviews_count: Long, unique_users: Array[Byte], unique_users_count: Long, avg_pageviews: Double,
                                  avg_ts_session: Double, anon_avg_ts_session: Double, new_user_count: Long,
                                  percent_new_users_count: Double, updated_date: Long) extends AlgoOutput with CassandraTable
case class PortalUsageSummaryFact_T(d_period: Int, d_author_id: String, d_app_id: String, d_channel: String, last_gen_date: Long, anon_total_sessions: Long, anon_total_ts: Double,
                                    total_sessions: Long, total_ts: Double, ce_total_sessions: Long, ce_percent_sessions: Double,
                                    total_pageviews_count: Long, unique_users: List[String], unique_users_count: Long, avg_pageviews: Double,
                                    avg_ts_session: Double, anon_avg_ts_session: Double, new_user_count: Long,
                                    percent_new_users_count: Double)

/**
 * @dataproduct
 * @updater
 *
 * UpdatePortalUsageDB
 *
 * Functionality
 * 1. Updater to populate/update the portal usage metrics per day, week, month & cumulative in Cassandra and influx DB.
 * Events used - ME_APP_USAGE_SUMMARY
 */
object UpdateAppUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, PortalUsageSummaryFact, PortalUsageSummaryIndex] with IInfluxDBUpdater with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateAppUsageDB"
    override def name: String = "UpdateAppUsageDB"
    val APP_USAGE_METRICS = "app_usage_metrics";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_APP_USAGE_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalUsageSummaryFact] = {

        val portalSummary = data.map { x =>

            val period = x.dimensions.period.get;
            val authorId = x.dimensions.author_id.get;
            val appId = CommonUtil.getAppDetails(x).id
            val channel = CommonUtil.getChannelId(x)

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]

            val anon_total_sessions = eksMap.get("anon_total_sessions").get.asInstanceOf[Number].longValue()
            val anon_total_ts = eksMap.get("anon_total_ts").get.asInstanceOf[Double]
            val total_sessions = eksMap.get("total_sessions").get.asInstanceOf[Number].longValue()
            val total_ts = eksMap.get("total_ts").get.asInstanceOf[Double]
            val ce_total_sessions = eksMap.get("ce_total_sessions").get.asInstanceOf[Number].longValue()
            val ce_percent_sessions = eksMap.get("ce_percent_sessions").get.asInstanceOf[Double]
            val total_pageviews_count = eksMap.get("total_pageviews_count").get.asInstanceOf[Number].longValue()
            val unique_users = eksMap.get("unique_users").get.asInstanceOf[List[String]]
            val unique_users_count = eksMap.get("unique_users_count").get.asInstanceOf[Number].longValue()
            val avg_pageviews = eksMap.get("avg_pageviews").get.asInstanceOf[Double]
            val avg_ts_session = eksMap.get("avg_ts_session").get.asInstanceOf[Double]
            val anon_avg_ts_session = eksMap.get("anon_avg_ts_session").get.asInstanceOf[Double]
            val new_user_count = eksMap.get("new_user_count").get.asInstanceOf[Number].longValue()
            val percent_new_users_count = eksMap.get("percent_new_users_count").get.asInstanceOf[Double]

            PortalUsageSummaryFact_T(period, authorId, appId, channel, x.context.date_range.to, anon_total_sessions, anon_total_ts, total_sessions, total_ts, ce_total_sessions, ce_percent_sessions,
                total_pageviews_count, unique_users, unique_users_count, avg_pageviews, avg_ts_session, anon_avg_ts_session, new_user_count, percent_new_users_count);
        }.cache();

        // Roll up summaries
        rollup(portalSummary, DAY).union(rollup(portalSummary, WEEK)).union(rollup(portalSummary, MONTH)).union(rollup(portalSummary, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[PortalUsageSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalUsageSummaryIndex] = {
        // Update the database (cassandra and influx)
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.APP_USAGE_SUMMARY_FACT)
        // Save to influx by filtering cumulative record
        val saveInfluxFlag = config.getOrElse("saveToInflux", false).asInstanceOf[Boolean];
        if(saveInfluxFlag) saveToInfluxDB(data);
        data.map { x => PortalUsageSummaryIndex(x.d_period, x.d_author_id, x.d_app_id, x.d_channel) };
    }

    private def rollup(data: RDD[PortalUsageSummaryFact_T], period: Period): RDD[PortalUsageSummaryFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.last_gen_date, period);
            (PortalUsageSummaryIndex(d_period, x.d_author_id, x.d_app_id, x.d_channel), PortalUsageSummaryFact_T(d_period, x.d_author_id, x.d_app_id, x.d_channel, x.last_gen_date, x.anon_total_sessions, x.anon_total_ts, x.total_sessions, x.total_ts, x.ce_total_sessions, x.ce_percent_sessions,
                x.total_pageviews_count, x.unique_users, x.unique_users_count, x.avg_pageviews, x.avg_ts_session, x.anon_avg_ts_session, x.new_user_count, x.percent_new_users_count));
        }.reduceByKey(reducePUS);
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[PortalUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.APP_USAGE_SUMMARY_FACT).on(SomeColumns("d_period", "d_author_id", "d_app_id", "d_channel"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(PortalUsageSummaryFact(index.d_period, index.d_author_id, index.d_app_id, index.d_channel, 0L, 0.0, 0L, 0.0, 0L, 0.0, 0L, BloomFilterUtil.getDefaultBytes(period), 0L, 0.0, 0.0, 0.0, 0L, 0.0, 0L));
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    private def reducePUS(fact1: PortalUsageSummaryFact_T, fact2: PortalUsageSummaryFact_T): PortalUsageSummaryFact_T = {
        val anon_total_sessions = fact2.anon_total_sessions + fact1.anon_total_sessions
        val anon_total_ts = CommonUtil.roundDouble(fact2.anon_total_ts + fact1.anon_total_ts, 2);
        val total_sessions = fact2.total_sessions + fact1.total_sessions
        val total_ts = CommonUtil.roundDouble(fact2.total_ts + fact1.total_ts, 2);
        val ce_total_sessions = fact2.ce_total_sessions + fact1.ce_total_sessions
        val ce_percent_sessions = if (ce_total_sessions == 0 || total_sessions == 0) 0d else CommonUtil.roundDouble(((ce_total_sessions / (total_sessions * 1d)) * 100), 2);
        val total_pageviews_count = fact2.total_pageviews_count + fact1.total_pageviews_count
        val unique_users = (fact2.unique_users ++ fact1.unique_users).distinct
        val unique_users_count = unique_users.length.toLong
        val avg_pageviews = if (total_pageviews_count == 0 || total_sessions == 0) 0d else CommonUtil.roundDouble((total_pageviews_count / (total_sessions * 1d)), 2);
        val avg_ts_session = if (total_ts == 0 || total_sessions == 0) 0d else CommonUtil.roundDouble((total_ts / total_sessions), 2);
        val anon_avg_ts_session = if (anon_total_ts == 0 || anon_total_sessions == 0) 0d else CommonUtil.roundDouble((anon_total_ts / anon_total_sessions), 2);
        val new_user_count = fact2.new_user_count + fact1.new_user_count
        val percent_new_users_count = if (new_user_count == 0 || unique_users_count == 0) 0d else CommonUtil.roundDouble(((new_user_count / (unique_users_count * 1d)) * 100), 2);

        PortalUsageSummaryFact_T(fact1.d_period, fact1.d_author_id, fact1.d_app_id, fact1.d_channel, fact1.last_gen_date, anon_total_sessions, anon_total_ts, total_sessions, total_ts, ce_total_sessions, ce_percent_sessions,
            total_pageviews_count, unique_users, unique_users_count, avg_pageviews, avg_ts_session, anon_avg_ts_session, new_user_count, percent_new_users_count);
    }

    private def reduce(fact1: PortalUsageSummaryFact, fact2: PortalUsageSummaryFact_T, period: Period): PortalUsageSummaryFact = {
        val anon_total_sessions = fact2.anon_total_sessions + fact1.anon_total_sessions
        val anon_total_ts = CommonUtil.roundDouble(fact2.anon_total_ts + fact1.anon_total_ts, 2);
        val total_sessions = fact2.total_sessions + fact1.total_sessions
        val total_ts = CommonUtil.roundDouble(fact2.total_ts + fact1.total_ts, 2);
        val ce_total_sessions = fact2.ce_total_sessions + fact1.ce_total_sessions
        val ce_percent_sessions = if (ce_total_sessions == 0 || total_sessions == 0) 0d else CommonUtil.roundDouble(((ce_total_sessions / (total_sessions * 1d)) * 100), 2);
        val total_pageviews_count = fact2.total_pageviews_count + fact1.total_pageviews_count
        val avg_pageviews = if (total_pageviews_count == 0 || total_sessions == 0) 0d else CommonUtil.roundDouble((total_pageviews_count / (total_sessions * 1d)), 2);
        val avg_ts_session = if (total_ts == 0 || total_sessions == 0) 0d else CommonUtil.roundDouble((total_ts / total_sessions), 2);
        val anon_avg_ts_session = if (anon_total_ts == 0 || anon_total_sessions == 0) 0d else CommonUtil.roundDouble((anon_total_ts / anon_total_sessions), 2);
        val new_user_count = fact2.new_user_count + fact1.new_user_count
        val bf = BloomFilterUtil.deserialize(period, fact1.unique_users);
        val userCount = BloomFilterUtil.countMissingValues(bf, fact2.unique_users);
        val unique_users_count = userCount + fact1.unique_users_count;
        val unique_users = BloomFilterUtil.serialize(bf);
        val percent_new_users_count = if (new_user_count == 0 || unique_users_count == 0) 0d else CommonUtil.roundDouble(((new_user_count / (unique_users_count * 1d)) * 100), 2);

        PortalUsageSummaryFact(fact1.d_period, fact1.d_author_id, fact1.d_app_id, fact1.d_channel, anon_total_sessions, anon_total_ts, total_sessions, total_ts, ce_total_sessions, ce_percent_sessions,
            total_pageviews_count, unique_users, unique_users_count, avg_pageviews, avg_ts_session, anon_avg_ts_session, new_user_count, percent_new_users_count, System.currentTimeMillis());
    }

    private def saveToInfluxDB(data: RDD[PortalUsageSummaryFact])(implicit sc: SparkContext) {
        val metrics = data.filter { x => x.d_period != 0 }.map { x =>
            val fields = (CommonUtil.caseClassToMap(x) - ("d_period", "d_author_id", "d_app_id", "d_channel", "unique_users", "updated_date")).map(f => (f._1, f._2.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef]));
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("period" -> time._2, "author_id" -> x.d_author_id, "app_id" -> x.d_app_id, "channel" -> x.d_channel), fields, time._1);
        };
        val authors  = getDenormalizedData("User", data.map { x => x.d_author_id })
        metrics.denormalize("author_id", "author_name", authors).saveToInflux(APP_USAGE_METRICS);
    }

}