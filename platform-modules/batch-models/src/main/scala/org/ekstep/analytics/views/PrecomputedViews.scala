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
import org.ekstep.analytics.util._
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.Empty
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.IBatchModel
import org.apache.spark.rdd.EmptyRDD

case class View(keyspace: String, table: String, periodUpTo: Int, periodType: String, filePrefix: String, fileSuffix: String, dispatchTo: String, dispatchParams: Map[String, AnyRef]);

object PrecomputedViews extends IBatchModel[String,String] with Serializable {

	implicit val className = "org.ekstep.analytics.views.PrecomputedViews"
	 override def name(): String = "PrecomputedViews";
	
    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String] ={
        precomputeContentUsageMetrics();
        precomputeContentPopularityMetrics();
        precomputeGenieLaunchMetrics();
        precomputeItemUsageMetrics();
        events
    }
    
    def precomputeContentUsageMetrics()(implicit sc: SparkContext) {

        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val groupFn = (x: ContentUsageSummaryView) => { (x.d_tag + "-" + x.d_content_id + "-" + x.d_channel) };
        precomputeMetrics[ContentUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 7, "DAY", AppConf.getConfig("pc_files_prefix") + "cus", "7DAYS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 5, "WEEK", AppConf.getConfig("pc_files_prefix") +"cus", "5WEEKS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 12, "MONTH", AppConf.getConfig("pc_files_prefix") +"cus", "12MONTHS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, 1, "CUMULATIVE", AppConf.getConfig("pc_files_prefix") + "cus", "CUMULATIVE.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
    }
    
    def precomputeContentPopularityMetrics()(implicit sc: SparkContext) {

        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val groupFn = (x: ContentPopularitySummaryView) => { (x.d_tag + "-" + x.d_content_id + "-" + x.d_channel) };
        precomputeMetrics[ContentPopularitySummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 7, "DAY", AppConf.getConfig("pc_files_prefix") + "cps", "7DAYS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentPopularitySummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 5, "WEEK", AppConf.getConfig("pc_files_prefix") +"cps", "5WEEKS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentPopularitySummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 12, "MONTH", AppConf.getConfig("pc_files_prefix") +"cps", "12MONTHS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ContentPopularitySummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT, 1, "CUMULATIVE", AppConf.getConfig("pc_files_prefix") + "cps", "CUMULATIVE.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
    }

    def precomputeGenieLaunchMetrics()(implicit sc: SparkContext) {

        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val groupFn = (x: GenieLaunchSummaryView) => { x.d_tag + "-" + x.d_channel };
        precomputeMetrics[GenieLaunchSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 7, "DAY", AppConf.getConfig("pc_files_prefix") + "gls", "7DAYS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[GenieLaunchSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 5, "WEEK", AppConf.getConfig("pc_files_prefix") +"gls", "5WEEKS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[GenieLaunchSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 12, "MONTH", AppConf.getConfig("pc_files_prefix") +"gls", "12MONTHS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[GenieLaunchSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, 1, "CUMULATIVE", AppConf.getConfig("pc_files_prefix") + "gls", "CUMULATIVE.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
    }
    
    def precomputeItemUsageMetrics()(implicit sc: SparkContext) {
    	val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val groupFn = (x: ItemUsageSummaryView) => { (x.d_tag + "-" + x.d_content_id + "-" + x.d_channel) };
        precomputeMetrics[ItemUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT, 7, "DAY", AppConf.getConfig("pc_files_prefix") + "ius", "7DAYS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ItemUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT, 5, "WEEK", AppConf.getConfig("pc_files_prefix") +"ius", "5WEEKS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ItemUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT, 12, "MONTH", AppConf.getConfig("pc_files_prefix") +"ius", "12MONTHS.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        precomputeMetrics[ItemUsageSummaryView](View(Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT, 1, "CUMULATIVE", AppConf.getConfig("pc_files_prefix") + "ius", "CUMULATIVE.json", AppConf.getConfig("pc_files_cache"), dispatchParams), groupFn);
        
    }

    def precomputeMetrics[T <: CassandraTable](view: View, groupFn: (T) => String)(implicit mf: Manifest[T], sc: SparkContext){
        val results = QueryProcessor.processQuery[T](view, groupFn);

        val count = results.map { x =>
            val fileKey = view.filePrefix + "-" + x._1 + "-" + view.fileSuffix;
            OutputDispatcher.dispatch(Dispatcher(view.dispatchTo, view.dispatchParams ++ Map("key" -> fileKey, "file" -> fileKey)), x._2)
        }.count();
        val data = CommonUtil.caseClassToMap(view);
        JobLogger.log("Precomputed metrics pushed.", Option(data ++ Map("count" -> count)), INFO);
    }

}