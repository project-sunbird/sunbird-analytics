package org.ekstep.analytics.model

import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer, RFormula }
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.feature.{ CountVectorizerModel, CountVectorizer, RegexTokenizer }
import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework._
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.adapter.ContentModel
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.sql.SQLContext
import scala.util.Random
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.sql.execution.stat.StatFunctions
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._
import org.apache.spark.mllib.util.MLUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.mllib.linalg.Vectors
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.joda.time.DateTime
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.dispatcher.FileDispatcher
import breeze.stats._
import java.io.File
import org.ekstep.analytics.updater.DeviceSpec
import org.ekstep.analytics.transformer._
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.LabeledPoint
import sys.process._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.adapter.ContentResponse

case class DeviceMetrics(did: DeviceId, content_list: Map[String, ContentModel], device_usage: DeviceUsageSummary, device_spec: DeviceSpec, device_content: Map[String, DeviceContentSummary], dcT: Map[String, dcus_tf]);
case class DeviceContext(did: String, contentInFocus: String, contentInFocusModel: ContentModel, contentInFocusVec: ContentToVector, contentInFocusUsageSummary: DeviceContentSummary, contentInFocusSummary: cus_t, otherContentId: String, otherContentModel: ContentModel, otherContentModelVec: ContentToVector, otherContentUsageSummary: DeviceContentSummary, otherContentSummary: ContentUsageSummaryFact, device_usage: DeviceUsageSummary, device_spec: DeviceSpec, otherContentSummaryT: cus_t, dusT: dus_tf, dcusT: dcus_tf, lastPlayedContentVec: ContentToVector) extends AlgoInput with AlgoOutput with Output;
case class DeviceRecos(device_id: String, scores: List[(String, Option[Double])]) extends AlgoOutput with Output
case class ContentToVector(contentId: String, text_vec: Option[List[Double]], tag_vec: Option[List[Double]]);

case class DeviceContextWithoutTransformations(did: String, contentInFocus: String, contentInFocusModel: ContentModel, contentInFocusVec: ContentToVector, contentInFocusUsageSummary: DeviceContentSummary, contentInFocusSummary: ContentUsageSummaryFact, otherContentId: String, otherContentModel: ContentModel, otherContentModelVec: ContentToVector, otherContentUsageSummary: DeviceContentSummary, otherContentSummary: ContentUsageSummaryFact, device_usage: DeviceUsageSummary, device_spec: DeviceSpec, otherContentSummaryT: cus_t, dusT: dus_tf, dcusT: dcus_tf, c3_contentVec: ContentToVector) extends AlgoInput with AlgoOutput with Output;
case class cus_t(t_publish_date: Option[Double], t_last_sync_date: Option[Double], t_total_ts: Option[Double], t_total_sessions: Option[Double], t_avg_ts_session: Option[Double], t_total_interactions: Option[Double], t_avg_interactions_min: Option[Double], t_total_devices: Option[Double], t_avg_sess_device: Option[Double]);
case class dus_tf(t_start_time: Option[Double], t_end_time: Option[Double], t_num_days: Option[Double], t_total_launches: Option[Double], t_total_timespent: Option[Double],
                  t_avg_num_launches: Option[Double], t_avg_time: Option[Double], t_num_contents: Option[Double], t_play_start_time: Option[Double], t_last_played_on: Option[Double],
                  t_total_play_time: Option[Double], t_num_sessions: Option[Double], t_mean_play_time: Option[Double],
                  t_mean_play_time_interval: Option[Double]);
case class dcus_tf(content_id: String, t_num_sessions: Option[Double], t_total_interactions: Option[Double], t_avg_interactions_min: Option[Double],
                   t_total_timespent: Option[Double], t_last_played_on: Option[Double], t_start_time: Option[Double],
                   t_mean_play_time_interval: Option[Double], t_download_date: Option[Double], t_num_group_user: Option[Double], t_num_individual_user: Option[Double]);

object DeviceRecommendationTrainingModel extends IBatchModelTemplate[DerivedEvent, DeviceContext, Empty, Empty] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DeviceRecommendationTrainingModel"
    override def name(): String = "DeviceRecommendationTrainingModel"

    val defaultDCUS = DeviceContentSummary(null, null, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultDUS = DeviceUsageSummary(null, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultCUS = ContentUsageSummaryFact(0, null, null, new DateTime(0), new DateTime(0), new DateTime(0), 0.0, 0L, 0.0, 0L, 0.0, 0, 0.0, null);
    val dateTime = new DateTime()
    val date = dateTime.toLocalDate()
    val time = dateTime.toLocalTime().toString("hh-mm")
    val path = "/training/" + date + "/" + time + "/"

    def choose[A](it: Buffer[A], r: Random): A = {
        val random_index = r.nextInt(it.size);
        it(random_index);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContext] = {

        val num_bins = config.getOrElse("num_bins", 4).asInstanceOf[Int];
        val filterByNumContents = config.getOrElse("filterByNumContents", false).asInstanceOf[Boolean];

        // Content Usage Summaries
        val contentUsageSummaries = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period=? and d_tag = 'all'", 0).map { x => x }.cache();
        // cus transformations
        val cusT = ContentUsageTransformer.getTransformationByBinning(contentUsageSummaries, num_bins)
        val contentUsageB = cusT.map { x => (x._1, x._2) }.collect().toMap
        val contentUsageBB = sc.broadcast(contentUsageB);
        val contentUsageO = contentUsageSummaries.map { x => (x.d_content_id, x) }.collect().toMap
        val contentUsageOB = sc.broadcast(contentUsageO);
        contentUsageSummaries.unpersist(true);

        //Content Model
        val contentModel = ContentAdapter.getPublishedContentForRE().map { x => (x.id, x) }.toMap;
        val contentIds = contentModel.map(f => f._1).toArray;
        JobLogger.log("Content count", Option(Map("count" -> contentModel.size)), INFO, "org.ekstep.analytics.model");
        val contentModelB = sc.broadcast(contentModel);
        
        //ContentToVector
        val contentVectors = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC).map { x => (x.contentId, x) }.collect().toMap;
        val contentVectorsB = sc.broadcast(contentVectors);
        
        // device-specifications
        val device_spec = sc.cassandraTable[DeviceSpec](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE).map { x => (DeviceId(x.device_id), x) }
        val allDevices = device_spec.map(x => x._1).distinct; // TODO: Do we need distinct here???
        
        // Device Usage Summaries
        val device_usage = allDevices.joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => x._2 }//.filter { x => x.num_contents.getOrElse(0L)>5 } //.map { x => (x._1, x._2) }

        // Filter by num_contents>5 logic in dus
        val final_dus_dsp = if (filterByNumContents) {
            val filtered_device_usage = device_usage.filter { x => x.num_contents.getOrElse(0L) > 5 }
            val filtered_devices = filtered_device_usage.map { x => x.device_id }.collect()
            val f_device_spec = device_spec.filter(f => filtered_devices.contains(f._1.device_id))
            JobLogger.log("Device Usage with num_contents > 5 count", Option(Map("count" -> filtered_device_usage.count())), INFO, "org.ekstep.analytics.model");
            (filtered_device_usage, f_device_spec)
        }
        else (device_usage, device_spec)
        JobLogger.log("Check for dus & dsp count in unfilter and filter for num_contents>5", Option(Map("unfiltered_dus" -> device_usage.count(), "unfiltered_dsp" -> device_spec.count(), "filtered_dus" -> final_dus_dsp._1.count(), "filtered_dsp" -> final_dus_dsp._2.count(), "memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        // device_spec transformations
        val device_specT = DeviceSpecTransformer.getTransformationByBinning(final_dus_dsp._2.map { x => x._2 }, num_bins).map { x => (DeviceId(x._1), x._2) }
        
        // dus transformations
        val dusT = DeviceUsageTransformer.getTransformationByBinning(final_dus_dsp._1, num_bins)
        val dusB = dusT.map { x => (x._1, x._2) }.collect().toMap;
        val dusBB = sc.broadcast(dusB);
        val dusO = device_usage.map { x => (DeviceId(x.device_id), x) }

        // Device Content Usage Summaries
        val dcus = sc.cassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).filter { x => contentIds.contains(x.content_id) }.cache();
        // dcus transformations
        val dcusT = DeviceContentUsageTransformer.getTransformationByBinning(dcus, num_bins)
        val dcusB = dcusT.map { x => (x._1, x._2) }.groupBy(f => f._1).mapValues(f => f.map(x => x._2)).collect().toMap;
        val dcusBB = sc.broadcast(dcusB);
        val dcusO = dcus.map { x => (DeviceId(x.device_id), x) }.groupBy(f => f._1).mapValues(f => f.map(x => x._2));

        dcus.unpersist(true);

        device_specT.leftOuterJoin(dusO).leftOuterJoin(dcusO).map { x =>
            val dc = x._2._2.getOrElse(Buffer[DeviceContentSummary]()).map { x => (x.content_id, x) }.toMap;
            val dcT = dcusBB.value.getOrElse(x._1.device_id, Buffer[dcus_tf]()).map { x => (x.content_id, x) }.toMap;
            DeviceMetrics(x._1, contentModelB.value, x._2._1._2.getOrElse(defaultDUS), x._2._1._1, dc, dcT)
        }.map { x =>
            val rand = new Random(System.currentTimeMillis());
            x.content_list.map { y =>
                {
                    val randomContent: DeviceContentSummary = if (x.device_content.isEmpty) defaultDCUS else choose[(String, DeviceContentSummary)](x.device_content.toBuffer, rand)._2;
                    val dcusT: dcus_tf = if (x.dcT.isEmpty) dcus_tf(null, None, None, None, None, None, None, None, None, None, None) else choose[(String, dcus_tf)](x.dcT.toBuffer, rand)._2;
                    val otherContentId: String = randomContent.content_id;
                    val otherContentModel: ContentModel = if (null != otherContentId) contentModelB.value.getOrElse(otherContentId, null) else null;
                    val contentInFocusVec = contentVectorsB.value.getOrElse(y._1, null);
                    val otherContentModelVec = if (null != otherContentId) contentVectorsB.value.getOrElse(randomContent.content_id, null) else null;
                    val lastPlayedContent = x.device_usage.last_played_content.getOrElse(null)
                    val lastPlayedContentVec = if (null != lastPlayedContent) contentVectorsB.value.getOrElse(lastPlayedContent, null) else null;
                    val contentInFocusUsageSummary = x.device_content.getOrElse(y._1, defaultDCUS);
                    val contentInFocusSummary = contentUsageBB.value.getOrElse(y._1, cus_t(None, None, None, None, None, None, None, None, None))
                    val otherContentSummary = if (null != otherContentId) contentUsageOB.value.getOrElse(otherContentId, defaultCUS) else defaultCUS;
                    val otherContentSummaryT = if (null != otherContentId) contentUsageBB.value.getOrElse(otherContentId, cus_t(None, None, None, None, None, None, None, None, None)) else cus_t(None, None, None, None, None, None, None, None, None);
                    val dusT = dusBB.value.getOrElse(x.did.device_id, dus_tf(None, None, None, None, None, None, None, None, None, None, None, None, None, None));
                    DeviceContext(x.did.device_id, y._1, y._2, contentInFocusVec, contentInFocusUsageSummary, contentInFocusSummary, otherContentId, otherContentModel, otherContentModelVec, randomContent, otherContentSummary, x.device_usage, x.device_spec, otherContentSummaryT, dusT, dcusT, lastPlayedContentVec);
                }
            }
        }.flatMap { x => x.map { f => f } }

    }

    private def _createDF(data: RDD[DeviceContext], tag_dimensions: Int, text_dimensions: Int): RDD[Row] = {
        data.map { x =>
            val seq = ListBuffer[Any]();
            //seq += x.did; // did
            // Add c1 text vectors
            seq ++= (if (null != x.contentInFocusVec && x.contentInFocusVec.text_vec.isDefined) {
                x.contentInFocusVec.text_vec.get.toSeq
            } else {
                _getZeros(text_dimensions);
            })
            // Add c1 tag vectors
            seq ++= (if (null != x.contentInFocusVec && x.contentInFocusVec.tag_vec.isDefined) {
                x.contentInFocusVec.tag_vec.get.toSeq
            } else {
                _getZeros(tag_dimensions);
            })
            // Add c1 context attributes
            val targetVariable = x.contentInFocusUsageSummary.total_timespent.getOrElse(0.0)
            seq ++= Seq(if (targetVariable == 0.0) targetVariable else CommonUtil.roundDouble(Math.log(targetVariable), 2))
            //            seq ++= Seq(x.contentInFocusUsageSummary.total_timespent.getOrElse(0.0), x.contentInFocusUsageSummary.avg_interactions_min.getOrElse(0.0),
            //                x.contentInFocusUsageSummary.download_date.getOrElse(0L), if (x.contentInFocusUsageSummary.downloaded.getOrElse(false)) 1 else 0, x.contentInFocusUsageSummary.last_played_on.getOrElse(0L),
            //                x.contentInFocusUsageSummary.mean_play_time_interval.getOrElse(0.0), x.contentInFocusUsageSummary.num_group_user.getOrElse(0L), x.contentInFocusUsageSummary.num_individual_user.getOrElse(0L),
            //                x.contentInFocusUsageSummary.num_sessions.getOrElse(0L), x.contentInFocusUsageSummary.start_time.getOrElse(0L), x.contentInFocusUsageSummary.total_interactions.getOrElse(0L))
            seq ++= Seq(x.contentInFocusModel.subject.mkString(","), x.contentInFocusModel.contentType, x.contentInFocusModel.languageCode.mkString(","))

            // Add c1 usage metrics

            seq ++= Seq(x.contentInFocusSummary.t_publish_date.getOrElse(0.0), x.contentInFocusSummary.t_last_sync_date.getOrElse(0.0), x.contentInFocusSummary.t_total_ts.getOrElse(0.0), x.contentInFocusSummary.t_total_sessions.getOrElse(0.0), x.contentInFocusSummary.t_avg_ts_session.getOrElse(0.0),
                x.contentInFocusSummary.t_total_interactions.getOrElse(0.0), x.contentInFocusSummary.t_avg_interactions_min.getOrElse(0.0), x.contentInFocusSummary.t_total_devices.getOrElse(0.0), x.contentInFocusSummary.t_avg_sess_device.getOrElse(0.0))

            // Add c2 text vectors
            seq ++= (if (null != x.otherContentModelVec && x.otherContentModelVec.text_vec.isDefined) {
                x.otherContentModelVec.text_vec.get.toSeq
            } else {
                _getZeros(text_dimensions);
            })
            // Add c2 tag vectors
            seq ++= (if (null != x.otherContentModelVec && x.otherContentModelVec.tag_vec.isDefined) {
                x.otherContentModelVec.tag_vec.get.toSeq
            } else {
                _getZeros(tag_dimensions);
            })

            // Add c2 context attributes
            seq ++= Seq(x.dcusT.t_total_timespent.getOrElse(0.0), x.dcusT.t_avg_interactions_min.getOrElse(0.0),
                x.dcusT.t_download_date.getOrElse(0.0), if (x.otherContentUsageSummary.downloaded.getOrElse(false)) 1 else 0, x.dcusT.t_last_played_on.getOrElse(0.0),
                x.dcusT.t_mean_play_time_interval.getOrElse(0.0), x.dcusT.t_num_group_user.getOrElse(0.0), x.dcusT.t_num_individual_user.getOrElse(0.0),
                x.dcusT.t_num_sessions.getOrElse(0.0), x.dcusT.t_start_time.getOrElse(0.0), x.dcusT.t_total_interactions.getOrElse(0.0))

            seq ++= (if (null != x.otherContentModel) {
                Seq(x.otherContentModel.subject.mkString(","), x.contentInFocusModel.contentType, x.contentInFocusModel.languageCode.mkString(","))
            } else {
                Seq("Unknown", "Unknown", "Unknown")
            })

            // Add c2 usage metrics

            seq ++= Seq(x.otherContentSummaryT.t_publish_date.getOrElse(0.0), x.otherContentSummaryT.t_last_sync_date.getOrElse(0.0), x.otherContentSummaryT.t_total_ts.getOrElse(0.0), x.otherContentSummaryT.t_total_sessions.getOrElse(0.0), x.otherContentSummaryT.t_avg_ts_session.getOrElse(0.0),
                x.otherContentSummaryT.t_total_interactions.getOrElse(0.0), x.otherContentSummaryT.t_avg_interactions_min.getOrElse(0.0), x.otherContentSummaryT.t_total_devices.getOrElse(0.0), x.otherContentSummaryT.t_avg_sess_device.getOrElse(0.0))

            //println(x.did, "x.device_usage.total_launches", x.device_usage.total_launches, x.device_usage.total_launches.getOrElse(0L).isInstanceOf[Long]);
            // TODO: x.device_usage.total_launches is being considered as Double - Debug further
            // Device Context Attributes

            seq ++= Seq(x.dusT.t_total_timespent.getOrElse(0.0), x.dusT.t_total_launches.getOrElse(0.0), x.dusT.t_total_play_time.getOrElse(0.0),
                x.dusT.t_avg_num_launches.getOrElse(0.0), x.dusT.t_avg_time.getOrElse(0.0), x.dusT.t_end_time.getOrElse(0.0),
                x.dusT.t_last_played_on.getOrElse(0.0), x.dusT.t_mean_play_time.getOrElse(0.0), x.dusT.t_mean_play_time_interval.getOrElse(0.0),
                x.dusT.t_num_contents.getOrElse(0.0), x.dusT.t_num_days.getOrElse(0.0), x.dusT.t_num_sessions.getOrElse(0.0), x.dusT.t_play_start_time.getOrElse(0.0),
                x.dusT.t_start_time.getOrElse(0.0))

            // Add lastPlayedContent(c3) text vectors
            seq ++= (if (null != x.lastPlayedContentVec && x.lastPlayedContentVec.text_vec.isDefined) {
                x.lastPlayedContentVec.text_vec.get.toSeq
            } else {
                _getZeros(text_dimensions);
            })
            // Add lastPlayedContent(c3) tag vectors
            seq ++= (if (null != x.lastPlayedContentVec && x.lastPlayedContentVec.tag_vec.isDefined) {
                x.lastPlayedContentVec.tag_vec.get.toSeq
            } else {
                _getZeros(tag_dimensions);
            })

            // Device Context Attributes
            seq ++= Seq(x.device_spec.make, x.device_spec.screen_size, x.device_spec.external_disk, x.device_spec.internal_disk)
            val psc = x.device_spec.primary_secondary_camera.split(",");
            if (psc.length == 0) {
                seq ++= Seq(0.0, 0.0);
            } else if (psc.length == 1) {
                seq ++= Seq(if (StringUtils.isBlank(psc(0))) 0.0 else psc(0).toDouble, 0.0);
            } else {
                seq ++= Seq(if (StringUtils.isBlank(psc(0))) 0.0 else psc(0).toDouble, if (StringUtils.isBlank(psc(1))) 0.0 else psc(1).toDouble);
            }

            Row.fromSeq(seq);
        }
    }

    private def _getStructType(tag_dimensions: Int, text_dimensions: Int): StructType = {
        val seq = ListBuffer[StructField]();
        //seq += new StructField("did", StringType, false); // did
        // Add c1 text vectors
        seq ++= _getStructField("c1_text", text_dimensions);
        // Add c1 tag vectors
        seq ++= _getStructField("c1_tag", tag_dimensions);
        // Add c1 context attributes
        seq ++= Seq(new StructField("c1_total_ts", DoubleType, true))
        //        seq ++= Seq(new StructField("c1_total_ts", DoubleType, true), new StructField("c1_avg_interactions_min", DoubleType, true), new StructField("c1_download_date", LongType, true),
        //            new StructField("c1_downloaded", IntegerType, true), new StructField("c1_last_played_on", LongType, true), new StructField("c1_mean_play_time_interval", DoubleType, true),
        //            new StructField("c1_num_group_user", LongType, true), new StructField("c1_num_individual_user", LongType, true), new StructField("c1_num_sessions", LongType, true),
        //            new StructField("c1_start_time", LongType, true), new StructField("c1_total_interactions", LongType, true));
        seq ++= Seq(new StructField("c1_subject", StringType, true), new StructField("c1_contentType", StringType, true), new StructField("c1_language", StringType, true));

        // Add c1 usage metrics

        seq ++= Seq(new StructField("c1_publish_date", DoubleType, true),
            new StructField("c1_last_sync_date", DoubleType, true), new StructField("c1_total_timespent", DoubleType, true), new StructField("c1_total_sessions", DoubleType, true),
            new StructField("c1_avg_ts_session", DoubleType, true), new StructField("c1_num_interactions", DoubleType, true), new StructField("c1_mean_interactions_min", DoubleType, true),
            new StructField("c1_total_devices", DoubleType, true), new StructField("c1_avg_sess_device", DoubleType, true));

        // Add c2 text vectors
        seq ++= _getStructField("c2_text", text_dimensions);
        // Add c2 tag vectors
        seq ++= _getStructField("c2_tag", tag_dimensions);
        // Add c2 context attributes

        seq ++= Seq(new StructField("c2_total_ts", DoubleType, true), new StructField("c2_avg_interactions_min", DoubleType, true), new StructField("c2_download_date", DoubleType, true),
            new StructField("c2_downloaded", IntegerType, true), new StructField("c2_last_played_on", DoubleType, true), new StructField("c2_mean_play_time_interval", DoubleType, true),
            new StructField("c2_num_group_user", DoubleType, true), new StructField("c2_num_individual_user", DoubleType, true), new StructField("c2_num_sessions", DoubleType, true),
            new StructField("c2_start_time", DoubleType, true), new StructField("c2_total_interactions", DoubleType, true))

        seq ++= Seq(new StructField("c2_subject", StringType, true), new StructField("c2_contentType", StringType, true), new StructField("c2_language", StringType, true))

        // Add c2 usage metrics

        seq ++= Seq(new StructField("c2_publish_date", DoubleType, true),
            new StructField("c2_last_sync_date", DoubleType, true), new StructField("c2_total_timespent", DoubleType, true), new StructField("c2_total_sessions", DoubleType, true),
            new StructField("c2_avg_ts_session", DoubleType, true), new StructField("c2_num_interactions", DoubleType, true), new StructField("c2_mean_interactions_min", DoubleType, true),
            new StructField("c2_total_devices", DoubleType, true), new StructField("c2_avg_sess_device", DoubleType, true));

        // Device Context Attributes
        seq ++= Seq(new StructField("total_timespent", DoubleType, true), new StructField("total_launches", DoubleType, true), new StructField("total_play_time", DoubleType, true),
            new StructField("avg_num_launches", DoubleType, true), new StructField("avg_time", DoubleType, true), new StructField("end_time", DoubleType, true),
            new StructField("last_played_on", DoubleType, true), new StructField("mean_play_time", DoubleType, true), new StructField("mean_play_time_interval", DoubleType, true),
            new StructField("num_contents", DoubleType, true), new StructField("num_days", DoubleType, true), new StructField("num_sessions", DoubleType, true),
            new StructField("play_start_time", DoubleType, true), new StructField("start_time", DoubleType, true))
        // Add c3 text vectors
        seq ++= _getStructField("c3_text", text_dimensions);
        // Add c3 tag vectors
        seq ++= _getStructField("c3_tag", tag_dimensions);

        // Device Specification
        seq ++= Seq(new StructField("device_spec", StringType, true), new StructField("screen_size", DoubleType, true), new StructField("external_disk", DoubleType, true), new StructField("internal_disk", DoubleType, true), new StructField("primary_camera", DoubleType, true), new StructField("secondary_camera", DoubleType, true))

        new StructType(seq.toArray);
    }

    private def _getZeros(max: Int): Seq[Double] = {
        (0 until max toList).map { x => x * 0.0 }.toSeq
    }

    private def _getStructField(prefix: String, max: Int): Seq[StructField] = {
        (0 until max toList).map { x => new StructField(prefix + x, DoubleType, true) }.toSeq
    }

    override def algorithm(data: RDD[DeviceContext], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

        JobLogger.log("Running the algorithm", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        implicit val sqlContext = new SQLContext(sc);

        import sqlContext.implicits._
        val localPath = config.getOrElse("localPath", "/tmp/RE-data/").asInstanceOf[String]
        val model_name = config.getOrElse("model_name", "fm.model").asInstanceOf[String]
        val completePath = localPath + path
        val trainDataFile = completePath + "trainData/part-00000"
        val testDataFile = completePath + "testData/part-00000"
        val logFile = completePath + "libfm.log"
        val libfmOut = completePath + "libfm.out"
        val featureDetailFile = completePath + "featureDetails"
        val model_path = completePath + model_name
        val libfmExec = config.getOrElse("libfm.executable_path", "/usr/local/bin/") + "libFM";
        val bucket = config.getOrElse("bucket", "sandbox-data-store").asInstanceOf[String];
        val key = config.getOrElse("key", "model/").asInstanceOf[String];
        val libFMTrainConfig = config.getOrElse("libFMTrainConfig", "-dim 1,1,0 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
        val trainRatio = config.getOrElse("trainRatio", 0.8).asInstanceOf[Double];
        val testRatio = config.getOrElse("testRatio", 0.2).asInstanceOf[Double];
        val dataLimit = config.getOrElse("dataLimit", -1).asInstanceOf[Int];
        val upload_model_s3 = config.getOrElse("upload_model_s3", true).asInstanceOf[Boolean];
        val tag_dimensions = config.getOrElse("tag_dimensions", 15).asInstanceOf[Int];
        val text_dimensions = config.getOrElse("text_dimensions", 15).asInstanceOf[Int];
        val saveFeatureFile = config.getOrElse("saveFeatureFile", false).asInstanceOf[Boolean];

        CommonUtil.deleteFile(trainDataFile);
        CommonUtil.deleteFile(testDataFile);
        CommonUtil.deleteFile(logFile);
        CommonUtil.deleteFile(model_path);

        JobLogger.log("Creating dataframe and libfm data", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val rdd: RDD[Row] = _createDF(data, tag_dimensions, text_dimensions);
        JobLogger.log("Creating RDD[Row]", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val df = sqlContext.createDataFrame(rdd, _getStructType(tag_dimensions, text_dimensions));
        JobLogger.log("Created dataframe and libfm data", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");

        if(saveFeatureFile){
            JobLogger.log("Writing features with distinct value count into a file", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            val columns = df.columns
            val z = columns.map{x => (x, df.select(x).distinct().count())}
            val features = sc.parallelize(z.map{x => x.toString().replace("(", "").replace(")", "")})
            features.coalesce(1,true).saveAsTextFile(featureDetailFile);
        }
        
        val formula = new RFormula()
            .setFormula("c1_total_ts ~ .")
            .setFeaturesCol("features")
            .setLabelCol("label")
        JobLogger.log("applied the formula", None, INFO, "org.ekstep.analytics.model");
        val output = formula.fit(df).transform(df)
        JobLogger.log("executing formula.fit(resultDF).transform(resultDF)", None, INFO, "org.ekstep.analytics.model");

        val labeledRDD = output.select("features", "label").map { x => new LabeledPoint(x.getDouble(1), x.getAs[org.apache.spark.ml.linalg.Vector](0)) }.rdd;
        JobLogger.log("created labeledRDD", None, INFO, "org.ekstep.analytics.model");

        val dataStr = labeledRDD.map {
            case LabeledPoint(label, features) =>

                val sb = new StringBuilder(label.toString)
                val sv = features.toSparse;
                val indices = sv.indices;
                val values = sv.values;
                sb += ' '
                sb ++= (0 until indices.length).map { x =>
                    (indices(x)).toString() + ":" + values(x).toString()
                }.toSeq.mkString(" ");

                sb.mkString
        }

        JobLogger.log("Creating training dataset", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val usedDataSet = dataStr.filter { x => !StringUtils.startsWith(x, "0.0") }

        JobLogger.log("sampling used datasets", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val sampledUsedDataSet = if (dataLimit == -1) usedDataSet else sc.parallelize(usedDataSet.take(dataLimit))
        
        // to-do: Need to check for trainRatio = testRatio = 0.5
        val trainDataSet = if(trainRatio == testRatio) sampledUsedDataSet.sample(false, trainRatio, System.currentTimeMillis().toInt) else sampledUsedDataSet.sample(false, trainRatio, System.currentTimeMillis().toInt);
        val testDataSet = if(trainRatio != testRatio) sampledUsedDataSet.sample(false, testRatio, System.currentTimeMillis().toInt) else null
        
        if(null != testDataSet){
            JobLogger.log("Dispatching train and test datasets", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            trainDataSet.coalesce(1,true).saveAsTextFile(completePath+"trainData");
            testDataSet.coalesce(1,true).saveAsTextFile(completePath+"testData");
        }
        else {
            JobLogger.log("Dispatching only train datasets since trainRatio=testRatio", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            trainDataSet.coalesce(1,true).saveAsTextFile(completePath+"trainData");
        }

        JobLogger.log("Running the training algorithm", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val script = if(null != testDataSet) s"$libfmExec -train $trainDataFile -test $testDataFile $libFMTrainConfig -rlog $logFile -out $libfmOut -save_model $model_path" else s"$libfmExec -train $trainDataFile -test $trainDataFile $libFMTrainConfig -rlog $logFile -out $libfmOut -save_model $model_path"
        ScriptDispatcher.dispatch(script)
        
        if (upload_model_s3) {
            JobLogger.log("Saving the model to S3", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            S3Dispatcher.dispatch(null, Map("filePath" -> model_path, "bucket" -> bucket, "key" -> (key + model_name)))
            JobLogger.log("Saved the model to S3", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        }
        sc.makeRDD(List(Empty()));
    }

    override def postProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        data;
    }
}
