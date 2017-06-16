package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.CassandraTable
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import com.datastax.spark.connector._
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.updater.PortalUsageSummaryFact
import org.ekstep.analytics.framework._
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.util.ItemUsageSummaryFact
import org.ekstep.analytics.updater._
import org.ekstep.analytics.updater.ContentSnapshotSummary
import org.ekstep.analytics.updater.ConceptSnapshotSummary
import org.ekstep.analytics.updater.AssetSnapshotSummary
import org.ekstep.analytics.updater.CEUsageSummaryFact
import org.ekstep.analytics.updater.TextbookSessionMetricsFact
import org.ekstep.analytics.updater.TextbookSnapshotSummary

case class ConfigDetails(keyspace: String, table: String, periodfrom: Long, periodUpTo: Long, filePrefix: String, fileSuffix: String, dispatchTo: String, dispatchParams: Map[String, AnyRef]);

object MetricsEventCreationModel extends IBatchModel[String,String] with Serializable {

    implicit val className = "org.ekstep.analytics.model.MetricsEventCreationModel"
    override def name(): String = "MetricsEventCreationModel";

    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String] ={
        
        val start_date = jobParams.getOrElse(Map()).getOrElse("start_date", 0L).asInstanceOf[Long];
        val end_date = jobParams.getOrElse(Map()).getOrElse("end_date", 0L).asInstanceOf[Long];
        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("metrics_dispatch_params"));
        
        computeContentUsageMetrics(start_date, end_date, dispatchParams);
        computeItemUsageMetrics(start_date, end_date, dispatchParams);
        computeGenieUsageMetrics(start_date, end_date, dispatchParams);
        computeContentSnapshotMetrics(start_date, end_date, dispatchParams);
        computeConceptSnapshotMetrics(start_date, end_date, dispatchParams);
        computeAssetSnapshotMetrics(start_date, end_date, dispatchParams)
        computeAppUsageMetrics(start_date, end_date, dispatchParams);
        computeCEUsageMetrics(start_date, end_date, dispatchParams);
        computeTextbookCreationMetrics(start_date, end_date, dispatchParams);
        computeTextbookSnapshotMetrics(start_date, end_date, dispatchParams);
        events
    }

    def computeContentUsageMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: ContentUsageSummaryFact) => { (x.d_period + "-" + x.d_tag + "-" + x.d_content_id) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "cus/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_CONTENT_USAGE_METRICS", "ContentUsageMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_tag", "d_content_id", "m_device_ids"), Dimensions(None, None, None, None, None, None, None, None, None, Option(f.d_tag), Option(f.d_period), Option(f.d_content_id), None, None, None, None, None, None, None, None, None, None, None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Content usage metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeItemUsageMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: ItemUsageSummaryFact) => { (x.d_period + "-" + x.d_tag + "-" + x.d_content_id + "-" + x.d_item_id) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "item-usage-summ/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_ITEM_USAGE_METRICS", "ItemUsageMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_tag", "d_content_id", "d_item_id"), Dimensions(None, None, None, None, None, None, None, None, None, Option(f.d_tag), Option(f.d_period), Option(f.d_content_id), None, Option(f.d_item_id), None, None, None, None, None, None, None, None, None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Item usage metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeGenieUsageMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: GenieUsageSummaryFact) => { (x.d_period + "-" + x.d_tag) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "genie-launch-summ/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_GENIE_USAGE_METRICS", "GenieUsageMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_tag", "m_device_ids"), Dimensions(None, None, None, None, None, None, None, None, None, Option(f.d_tag), Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Genie usage metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeContentSnapshotMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: ContentSnapshotSummary) => { (x.d_period + "-" + x.d_author_id + "-" + x.d_partner_id) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SNAPSHOT_SUMMARY, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "content-snapshot/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_CONTENT_SNAPSHOT_METRICS", "ContentSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_author_id", "d_partner_id"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, Option(f.d_author_id), Option(f.d_partner_id), None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Content Snapshot metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeConceptSnapshotMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: ConceptSnapshotSummary) => { (x.d_period + "-" + x.d_concept_id) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "concept-snapshot/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_CONCEPT_SNAPSHOT_METRICS", "ConceptSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_concept_id"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, Option(f.d_concept_id), None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Concept Snapshot metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeAssetSnapshotMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: AssetSnapshotSummary) => { (x.d_period + "-" + x.d_partner_id) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.ASSET_SNAPSHOT_SUMMARY, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "asset-snapshot/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_ASSET_SNAPSHOT_METRICS", "AssetSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_partner_id"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, Option(f.d_partner_id), None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Asset Snapshot metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeAppUsageMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: PortalUsageSummaryFact) => { (x.d_period + "-" + x.d_author_id + "-" + x.d_app_id) };
        val details = ConfigDetails(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.APP_USAGE_SUMMARY_FACT, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "app-usage/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(details, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_APP_USAGE_METRICS", "AppUsageMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_author_id", "d_app_id", "unique_users"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, Option(f.d_author_id), None, None, Option(f.d_app_id)))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, details)
        JobLogger.log("App usage metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeCEUsageMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: CEUsageSummaryFact) => { (x.d_period + "-" + x.d_content_id) };
        val details = ConfigDetails(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "ce-usage/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(details, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_CE_USAGE_METRICS", "CEUsageMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_content_id"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), Option(f.d_content_id), None, None, None, None, None, None, None, None, None, None, None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, details)
        JobLogger.log("CE usage metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeTextbookCreationMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: TextbookSessionMetricsFact) => { (x.d_period.toString()) };
        val details = ConfigDetails(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SESSION_METRICS_FACT, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "textbook-usage/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(details, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_TEXTBOOK_CREATION_METRICS", "TextbookCreationMetrics", CommonUtil.caseClassToMap(f) - ("d_period"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, details)
        JobLogger.log("Textbook creation metrics pushed.", Option(Map("count" -> count)), INFO);
    }
    
    def computeTextbookSnapshotMetrics(start_date: Long, end_date: Long, dispatchParams: Map[String, AnyRef])(implicit sc: SparkContext) {

        val groupFn = (x: TextbookSnapshotSummary) => { (x.d_period + "-" + x.d_textbook_id) };
        val details = ConfigDetails(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SNAPSHOT_METRICS_TABLE, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + "textbook-snapshot/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(details, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent("ME_TEXTBOOK_SNAPSHOT_METRICS", "TextbookSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_textbook_id"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(f.d_textbook_id)))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, details)
        JobLogger.log("Textbook snapshot metrics pushed.", Option(Map("count" -> count)), INFO);
    }

    def processQueryAndComputeMetrics[T <: CassandraTable](details: ConfigDetails, groupFn: (T) => String)(implicit mf: Manifest[T], sc: SparkContext): RDD[(String, Array[T])] = {
        val filterFn = (x: String) => { StringUtils.split(x, "-").apply(0).size == 8 };
        val rdd = sc.cassandraTable[T](details.keyspace, details.table).where("updated_date>=?", details.periodfrom).where("updated_date<=?", details.periodUpTo)
        val files = rdd.groupBy { x => groupFn(x) }.filter { x => filterFn(x._1) }.mapValues { x => x.toArray }
        files;
    }

    def getMeasuredEvent[T <: CassandraTable](eid: String, modelName: String, metrics: Map[String, AnyRef], dimension: Dimensions): MeasuredEvent = {

        val mid = CommonUtil.getMessageId(eid, null, "DAY", System.currentTimeMillis());
        MeasuredEvent(eid, System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, "", None, None,
            Context(PData("AnalyticsDataPipeline", modelName, "1.0"), None, "DAY", null),
            dimension,
            MEEdata(metrics));
    }
    
    def saveToS3(data: RDD[(String, Array[String])], details: ConfigDetails)(implicit sc: SparkContext): Long = {
        data.map { x =>
            val period = StringUtils.split(x._1, "-").apply(0)
            val periodDateTime = CommonUtil.dayPeriod.parseDateTime(period).withTimeAtStartOfDay()
            val periodString = StringUtils.split(periodDateTime.toString(), "T").apply(0)
            val ts = periodDateTime.getMillis
            val fileKey = details.filePrefix +  periodString + "-" + ts + details.fileSuffix;
            OutputDispatcher.dispatch(Dispatcher(details.dispatchTo, details.dispatchParams ++ Map("key" -> fileKey, "file" -> fileKey)), x._2)
        }.count();
    }
}