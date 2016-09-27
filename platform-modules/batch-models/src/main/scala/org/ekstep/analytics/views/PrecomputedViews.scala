package org.ekstep.analytics.views

import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.Dispatcher
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.CassandraTable
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils

case class View(keyspace: String, table: String, periodUpTo: Int, periodType: String, filePrefix: String, fileSuffix: String, dispatchTo: String, dispatchParams: Map[String, AnyRef]);
case class ContentUsageSummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_publish_date: Long, m_last_sync_date: Long, m_last_gen_date: Long, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double) extends CassandraTable
case class ContentPopularitySummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, DateTime)], m_ratings: List[(Double, DateTime)], m_avg_rating: Double) extends CassandraTable
case class GenieLaunchSummaryFact(d_period: Int, d_tag: String, m_total_sessions: Long, m_total_ts: Double, m_total_devices: Long, m_avg_sessions: Double, m_avg_ts: Long, m_contents: List[String]) extends CassandraTable

object PrecomputedViews {

    def execute()(implicit sc: SparkContext) {
        precomputeContentUsageMetrics();
        precomputeContentPopularityMetrics();
        precomputeGenieLaunchMetrics();
    }
    
    def precomputeContentUsageMetrics()(implicit sc: SparkContext) {

        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val groupFn = (x: ContentUsageSummaryFact) => { (x.d_tag + "-" + x.d_content_id) };
        precomputeMetrics[ContentUsageSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 7, "DAY", AppConf.getConfig("pc_files_prefix") + "cus", "7DAYS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentUsageSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 5, "WEEK", AppConf.getConfig("pc_files_prefix") +"cus", "5WEEKS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentUsageSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 12, "MONTH", AppConf.getConfig("pc_files_prefix") +"cus", "12MONTHS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentUsageSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 1, "CUMULATIVE", AppConf.getConfig("pc_files_prefix") + "cus", "CUMULATIVE.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
    }
    
    def precomputeContentPopularityMetrics()(implicit sc: SparkContext) {

        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val groupFn = (x: ContentPopularitySummaryFact) => { (x.d_tag + "-" + x.d_content_id) };
        precomputeMetrics[ContentPopularitySummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 7, "DAY", AppConf.getConfig("pc_files_prefix") + "cus", "7DAYS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentPopularitySummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 5, "WEEK", AppConf.getConfig("pc_files_prefix") +"cus", "5WEEKS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentPopularitySummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 12, "MONTH", AppConf.getConfig("pc_files_prefix") +"cus", "12MONTHS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentPopularitySummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 1, "CUMULATIVE", AppConf.getConfig("pc_files_prefix") + "cus", "CUMULATIVE.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
    }

    def precomputeGenieLaunchMetrics()(implicit sc: SparkContext) {

        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val groupFn = (x: GenieLaunchSummaryFact) => { x.d_tag };
        precomputeMetrics[GenieLaunchSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 7, "DAY", AppConf.getConfig("pc_files_prefix") + "cus", "7DAYS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[GenieLaunchSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 5, "WEEK", AppConf.getConfig("pc_files_prefix") +"cus", "5WEEKS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[GenieLaunchSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 12, "MONTH", AppConf.getConfig("pc_files_prefix") +"cus", "12MONTHS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[GenieLaunchSummaryFact](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 1, "CUMULATIVE", AppConf.getConfig("pc_files_prefix") + "cus", "CUMULATIVE.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
    }

    def precomputeMetrics[T <: CassandraTable](view: View, groupFn: (T) => String)(implicit mf: Manifest[T], sc: SparkContext) = {
        val results = QueryProcessor.processQuery[T](view, groupFn);

        results.map { x =>
            val fileKey = view.filePrefix + "-" + x._1 + "-" + view.fileSuffix;
            OutputDispatcher.dispatch(Dispatcher(view.dispatchTo, view.dispatchParams ++ Map("key" -> fileKey, "file" -> fileKey)), x._2)
        }.collect(); ;
    }

}