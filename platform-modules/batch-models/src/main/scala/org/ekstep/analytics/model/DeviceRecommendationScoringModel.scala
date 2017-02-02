package org.ekstep.analytics.model

import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer, RFormula }
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.mllib.linalg.Vector
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
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.joda.time.DateTime
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.dispatcher.FileDispatcher
import org.apache.spark.ml.feature.{ HashingTF, IDF, Tokenizer, RegexTokenizer, CountVectorizer, CountVectorizerModel }
import org.apache.spark.ml.feature.SQLTransformer
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.ekstep.analytics.framework.util.S3Util
import breeze.stats._
import org.ekstep.analytics.updater.DeviceSpec
import org.ekstep.analytics.transformer._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.adapter.ContentResponse
import java.io.File

case class IndexedScore(index: Long, score: Double)

object DeviceRecommendationScoringModel extends IBatchModelTemplate[DerivedEvent, DeviceContext, DeviceRecos, DeviceRecos] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DeviceRecommendationScoringModel"
    override def name(): String = "DeviceRecommendationScoringModel"

    val defaultDCUS = DeviceContentSummary(null, null, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultDUS = DeviceUsageSummary(null, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultCUS = ContentUsageSummaryFact(0, null, null, new DateTime(0), new DateTime(0), new DateTime(0), 0.0, 0L, 0.0, 0L, 0.0, 0, 0.0, null);
    val dateTime = new DateTime()
    val date = dateTime.toLocalDate()
    val time = dateTime.toLocalTime().toString("HH-mm")
    val path_default = "/scoring/" + date + "/" + time + "/"
    val indexArray = ListBuffer[Long]()

    def choose[A](it: Buffer[A], r: Random): A = {
        val random_index = r.nextInt(it.size);
        it(random_index);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContext] = {

        val num_bins = config.getOrElse("num_bins", 4).asInstanceOf[Int];
        val filterByNumContents = config.getOrElse("filterByNumContents", false).asInstanceOf[Boolean];
        val dataTimeFolderStructure = config.getOrElse("dataTimeFolderStructure", true).asInstanceOf[Boolean];
        val path = if(dataTimeFolderStructure) path_default else "" 

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
        val device_usage = allDevices.joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => x._2 }

        // Filter by num_contents>5 logic in dus
        val final_dus_dsp = if (filterByNumContents) {
            val filtered_device_usage = device_usage.filter { x => x.num_contents.getOrElse(0L) > 5 }
            val filtered_devices = filtered_device_usage.map { x => x.device_id }.collect()
            val f_device_spec = device_spec.filter(f => filtered_devices.contains(f._1.device_id))
            JobLogger.log("Device Usage with num_contents > 5 count", Option(Map("count" -> filtered_device_usage.count())), INFO, "org.ekstep.analytics.model");
            (filtered_device_usage, f_device_spec)
        } else (device_usage, device_spec)
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

        // creating DeviceContext without transformations
        val tag_dimensions = config.getOrElse("tag_dimensions", 15).asInstanceOf[Int];
        val text_dimensions = config.getOrElse("text_dimensions", 15).asInstanceOf[Int];
        val localPath = config.getOrElse("localPath", "/tmp/RE-data/").asInstanceOf[String]
        val inputDataPath = localPath + path + "RE-input"
        val deviceContext = createDeviceContextWithoutTransformation(final_dus_dsp._2, final_dus_dsp._1.map { x => (DeviceId(x.device_id), x) }, dcus.map { x => (DeviceId(x.device_id), x) }.groupBy(f => f._1).mapValues(f => f.map(x => x._2)), contentVectors, contentUsageSummaries.map { x => (x.d_content_id, x) }.collect().toMap, contentModel)
        val deviceContextWithIndex = deviceContext.zipWithIndex().map { case (k, v) => (v, k) }
        val deviceContextF = deviceContextWithIndex.filter { x => x._2.contentInFocusUsageSummary.total_timespent.getOrElse(0.0) > 0.0 }
        JobLogger.log("Saving index of DeviceContext with non zero ts", Option(Map("totalcount" -> deviceContextWithIndex.count(), "nonZeroCount" -> deviceContextF.count())), INFO, "org.ekstep.analytics.model");
        deviceContextF.foreach { x => indexArray += x._1 }
        JobLogger.log("saving input data in json format", Option(Map("indexArray count" -> indexArray.size, "memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val file = new File(inputDataPath)
        if (file.exists())
            CommonUtil.deleteDirectory(inputDataPath)
        val jsondata = createJSON(deviceContextF, tag_dimensions, text_dimensions)
        jsondata.saveAsTextFile(inputDataPath)

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

            seq ++= Seq(x.dusT.t_total_timespent.getOrElse(0.0), x.dusT.t_total_launches.getOrElse(0.0), x.dusT.t_total_play_time.getOrElse(0.0),
                x.dusT.t_avg_num_launches.getOrElse(0.0), x.dusT.t_avg_time.getOrElse(0.0), x.dusT.t_end_time.getOrElse(0.0),
                x.dusT.t_last_played_on.getOrElse(0.0), x.dusT.t_mean_play_time.getOrElse(0.0), x.dusT.t_mean_play_time_interval.getOrElse(0.0),
                x.dusT.t_num_contents.getOrElse(0.0), x.dusT.t_num_days.getOrElse(0.0), x.dusT.t_num_sessions.getOrElse(0.0), x.dusT.t_play_start_time.getOrElse(0.0),
                x.dusT.t_start_time.getOrElse(0.0))

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
        // Add c3 text vectors
        seq ++= _getStructField("c3_text", text_dimensions);
        // Add c3 tag vectors
        seq ++= _getStructField("c3_tag", tag_dimensions);
        
        seq ++= Seq(new StructField("total_timespent", DoubleType, true), new StructField("total_launches", DoubleType, true), new StructField("total_play_time", DoubleType, true),
            new StructField("avg_num_launches", DoubleType, true), new StructField("avg_time", DoubleType, true), new StructField("end_time", DoubleType, true),
            new StructField("last_played_on", DoubleType, true), new StructField("mean_play_time", DoubleType, true), new StructField("mean_play_time_interval", DoubleType, true),
            new StructField("num_contents", DoubleType, true), new StructField("num_days", DoubleType, true), new StructField("num_sessions", DoubleType, true),
            new StructField("play_start_time", DoubleType, true), new StructField("start_time", DoubleType, true))
        
        new StructType(seq.toArray);
    }

    private def _getZeros(max: Int): Seq[Double] = {
        (0 until max toList).map { x => x * 0.0 }.toSeq
    }

    private def _getStructField(prefix: String, max: Int): Seq[StructField] = {
        (0 until max toList).map { x => new StructField(prefix + x, DoubleType, true) }.toSeq
    }

    def scoringAlgo(data: RDD[org.apache.spark.ml.linalg.DenseVector], localPath: String, model: String)(implicit sc: SparkContext): RDD[Double] = {
        val modelData = sc.textFile(localPath + model).filter(!_.isEmpty()).collect()

        JobLogger.log("Fetching w0, wj, vj from model file", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val W0_indStart = modelData.indexOf("#global bias W0")
        val W_indStart = modelData.indexOf("#unary interactions Wj")
        val v_indStart = modelData.indexOf("#pairwise interactions Vj,f") + 1

        val w0 = if (W0_indStart == -1) 0.0 else modelData(W0_indStart + 1).toDouble

        val features = if (W_indStart == -1) Array[Double]() else modelData.slice(W_indStart + 1, v_indStart - 1).map(x => x.toDouble)
        val w = if (features.isEmpty) 0.0 else DenseMatrix.create(features.length, 1, features)

        val interactions = if (v_indStart == modelData.length) Array[String]() else modelData.slice(v_indStart, modelData.length)
        val v = if (interactions.isEmpty) 0.0
        else {
            var col = 0
            val test = interactions.map { x =>
                val arr = x.split(" ").map(x => if (x.equals("-nan")) 0.0 else x.toDouble)
                col = arr.length
                arr
            }
            val data = test.flatMap { x => x }
            DenseMatrix.create(col, test.length, data).t
        }

        JobLogger.log("generating scores", None, INFO, "org.ekstep.analytics.model");
        data.map { vect =>
            val x = DenseMatrix.create(1, vect.size, vect.toArray)
            val yhatW = if (w != 0.0) {
                val wi = w.asInstanceOf[DenseMatrix[Double]]
                val yhatw = (x * wi)
                yhatw.valueAt(0, 0)
            } else 0.0
            val xsq = x.:*(x)
            val yhatV = if (v != 0.0) {
                val vi = v.asInstanceOf[DenseMatrix[Double]]
                val vsq = vi.:*(vi)
                val v1 = x * vi
                val v1sq = v1.:*(v1)
                val v2 = xsq * vsq
                val vdiff = v1sq - v2
                0.5 * (breeze.linalg.sum(vdiff));
            } else 0.0

            CommonUtil.roundDouble((w0 + yhatW + yhatV), 2)
        }
    }

    override def algorithm(data: RDD[DeviceContext], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceRecos] = {

        JobLogger.log("Running the algorithm method", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus, "input count" -> data.count())), INFO, "org.ekstep.analytics.model");
        implicit val sqlContext = new SQLContext(sc);

        val localPath = config.getOrElse("localPath", "/tmp/RE-data/").asInstanceOf[String]
        val dataTimeFolderStructure = config.getOrElse("dataTimeFolderStructure", true).asInstanceOf[Boolean];
        val path = if(dataTimeFolderStructure) path_default else "" 
        val outputFile = localPath + path + "scores"
        val inputDataPath = localPath + path + "RE-input"
        val scoreDataPath = localPath + path + "RE-score"
        val model_name = config.getOrElse("model_name", "fm.model").asInstanceOf[String]
        val bucket = config.getOrElse("bucket", "sandbox-data-store").asInstanceOf[String];
        val key = config.getOrElse("key", "model/").asInstanceOf[String];
        val tag_dimensions = config.getOrElse("tag_dimensions", 15).asInstanceOf[Int];
        val text_dimensions = config.getOrElse("text_dimensions", 15).asInstanceOf[Int];
        val upload_score_s3 = config.getOrElse("upload_score_s3", false).asInstanceOf[Boolean];
        val saveScoresToFile = config.getOrElse("saveScoresToFile", true).asInstanceOf[Boolean];
        val filterBlacklistedContents = config.getOrElse("filterBlacklistedContents", true).asInstanceOf[Boolean];
        CommonUtil.deleteFile(localPath + key.split("/").last);

        JobLogger.log("Creating dataframe and libfm data", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val rdd: RDD[Row] = _createDF(data, tag_dimensions, text_dimensions);
        JobLogger.log("Creating RDD[Row]", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val df = sqlContext.createDataFrame(rdd, _getStructType(tag_dimensions, text_dimensions));
        JobLogger.log("Created dataframe and libfm data", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");

        val formula = new RFormula()
            .setFormula("c1_total_ts ~ .")
            .setFeaturesCol("features")
            .setLabelCol("label")
        JobLogger.log("applied the formula", None, INFO, "org.ekstep.analytics.model");
        val output = formula.fit(df).transform(df)
        JobLogger.log("executing formula.fit(resultDF).transform(resultDF)", None, INFO, "org.ekstep.analytics.model");
        
        val featureVector = output.select("features").rdd.map { x => x.getAs[org.apache.spark.ml.linalg.SparseVector](0).toDense }; //x.asInstanceOf[org.apache.spark.mllib.linalg.DenseVector] };
        JobLogger.log("created featureVector", None, INFO, "org.ekstep.analytics.model");

        JobLogger.log("Downloading model from s3", None, INFO, "org.ekstep.analytics.model");
        S3Util.downloadFile(bucket, key + model_name, localPath + path)

        JobLogger.log("Running scoring algorithm", None, INFO, "org.ekstep.analytics.model");
        val scores = scoringAlgo(featureVector, localPath + path, model_name);

        if (upload_score_s3) {
            JobLogger.log("Dispatching scores to a file", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            scores.coalesce(1, true).saveAsTextFile(outputFile);
            JobLogger.log("Saving the score file to S3", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            S3Dispatcher.dispatch(null, Map("filePath" -> outputFile, "bucket" -> bucket, "key" -> (key + "score.txt")))
        }

        JobLogger.log("Load the scores into memory and join with device content", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val device_content = data.map { x => (x.did, x.contentInFocus) }.zipWithIndex().map { case (k, v) => (v, k) }
        val scoresIndexed = scores.zipWithIndex().map { case (k, v) => (v, k) }
        JobLogger.log("device_content and scores with index count", Option(Map("device_content" -> device_content.count(), "scoresIndexed" -> scoresIndexed.count(), "memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val device_scores = device_content.leftOuterJoin(scoresIndexed).map { x => (x._2._1._1, x._2._1._2, x._2._2) }.groupBy(x => x._1).mapValues(f => f.map(x => (x._2, x._3)).toList.sortBy(y => y._2).reverse)
        JobLogger.log("Number of devices for which scoring is done", Option(Map("Scored_devices" -> device_scores.count(), "devices_in_spec" -> data.map { x => x.device_spec.device_id }.distinct().count(), "memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");

        if (saveScoresToFile) {
            val scoreData = device_content.leftOuterJoin(scoresIndexed).map { f =>
                IndexedScore(f._1, f._2._2.get)
            }.filter { x => indexArray.contains(x.index) }.map { x => JSONUtils.serialize(x) }
            JobLogger.log("Number of device-content for which saving scores to a file", Option(Map("saving_Scores_index count" -> scoreData.count(), "memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            scoreData.saveAsTextFile(scoreDataPath);
        }

        val final_scores = if (filterBlacklistedContents) {
            JobLogger.log("Fetching blacklisted contents from database", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            val blacklistedContents = sc.cassandraTable[BlacklistContents](Constants.PLATFORM_KEY_SPACE_NAME, Constants.RECOMMENDATION_CONFIG)
            val contentsList = blacklistedContents.filter { x => x.config_key.equals("device_reco_blacklist") }
            if (!contentsList.isEmpty()) {
                JobLogger.log("Filtering scores for blacklisted contents", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
                val contents = contentsList.first().config_value
                device_scores.map { x =>
                    val filteredlist = x._2.filterNot(f => contents.contains(f._1))
                    (x._1, filteredlist)
                }
            } else device_scores
        } else device_scores
        JobLogger.log("Check for score count in unfilter and filter for blacklisted contents for first content", Option(Map("unfiltered_scores" -> device_scores.first()._2.size, "filtered_scores_blacklisted" -> final_scores.first()._2.size, "memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        final_scores.map { x =>
            DeviceRecos(x._1, x._2)
        }
    }

    override def postProcess(data: RDD[DeviceRecos], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceRecos] = {

        val scoresTable = config.getOrElse("scoresTable", "device_recos_ds").asInstanceOf[String];
        JobLogger.log("Save the scores to cassandra", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        data.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, scoresTable)
        data;
    }

    def createDeviceContextWithoutTransformation(device_spec: RDD[(DeviceId, DeviceSpec)], dus: RDD[(DeviceId, DeviceUsageSummary)], dcus: RDD[(DeviceId, Iterable[DeviceContentSummary])], contentVectors: Map[String, ContentToVector], contentUsage: Map[String, ContentUsageSummaryFact], contentModel: Map[String, ContentModel])(implicit sc: SparkContext): RDD[DeviceContextWithoutTransformations] = {

        val contentModelB = sc.broadcast(contentModel)
        val contentVectorsB = sc.broadcast(contentVectors)
        val contentUsageB = sc.broadcast(contentUsage)

        device_spec.leftOuterJoin(dus).leftOuterJoin(dcus).map { x =>
            val dc = x._2._2.getOrElse(Buffer[DeviceContentSummary]()).map { x => (x.content_id, x) }.toMap;
            DeviceMetrics(x._1, contentModelB.value, x._2._1._2.getOrElse(defaultDUS), x._2._1._1, dc, null)
        }.map { x =>
            val rand = new Random(System.currentTimeMillis());
            x.content_list.map { y =>
                {
                    val randomContent: DeviceContentSummary = if (x.device_content.isEmpty) defaultDCUS else choose[(String, DeviceContentSummary)](x.device_content.toBuffer, rand)._2;
                    val otherContentId: String = randomContent.content_id;
                    val otherContentModel: ContentModel = if (null != otherContentId) contentModelB.value.getOrElse(otherContentId, null) else null;
                    val contentInFocusVec = contentVectorsB.value.getOrElse(y._1, null);
                    val otherContentModelVec = if (null != otherContentId) contentVectorsB.value.getOrElse(randomContent.content_id, null) else null;
                    val contentInFocusUsageSummary = x.device_content.getOrElse(y._1, defaultDCUS);
                    val contentInFocusSummary = contentUsageB.value.getOrElse(y._1, defaultCUS)
                    val otherContentSummary = if (null != otherContentId) contentUsageB.value.getOrElse(otherContentId, defaultCUS) else defaultCUS;
                    val c3_contentId = x.device_usage.last_played_content.getOrElse(null)
                    val c3_contentVec = if (null != c3_contentId) contentVectorsB.value.getOrElse(c3_contentId, null) else null;
                    DeviceContextWithoutTransformations(x.did.device_id, y._1, y._2, contentInFocusVec, contentInFocusUsageSummary, contentInFocusSummary, otherContentId, otherContentModel, otherContentModelVec, randomContent, otherContentSummary, x.device_usage, x.device_spec, null, null, null, c3_contentVec);
                }
            }
        }.flatMap { x => x.map { f => f } }
    }

    def createJSON(data: RDD[(Long, DeviceContextWithoutTransformations)], tag_dimensions: Int, text_dimensions: Int)(implicit sc: SparkContext): RDD[String] = {

        data.map { f =>
            val x = f._2
            val c1_text = if (null != x.contentInFocusVec && x.contentInFocusVec.text_vec.isDefined)
                x.contentInFocusVec.text_vec.get.toSeq
            else _getZeros(text_dimensions);

            val c1_tag = if (null != x.contentInFocusVec && x.contentInFocusVec.tag_vec.isDefined)
                x.contentInFocusVec.tag_vec.get.toSeq
            else _getZeros(tag_dimensions);
            val c2_text = if (null != x.otherContentModelVec && x.otherContentModelVec.text_vec.isDefined)
                x.otherContentModelVec.text_vec.get.toSeq
            else _getZeros(text_dimensions);
            val c2_tag = if (null != x.otherContentModelVec && x.otherContentModelVec.tag_vec.isDefined)
                x.otherContentModelVec.tag_vec.get.toSeq
            else _getZeros(tag_dimensions)
            val c3_text = if (null != x.c3_contentVec && x.c3_contentVec.text_vec.isDefined)
                x.c3_contentVec.text_vec.get.toSeq
            else _getZeros(text_dimensions);
            val c3_tag = if (null != x.c3_contentVec && x.c3_contentVec.tag_vec.isDefined)
                x.c3_contentVec.tag_vec.get.toSeq
            else _getZeros(tag_dimensions)
            val c2_downloaded = if (x.otherContentUsageSummary.downloaded.getOrElse(false)) 1 else 0
            val c2_subject = if (null != x.otherContentModel) x.otherContentModel.subject.mkString(",") else "Unknown"
            val c2_contentType = if (null != x.otherContentModel) x.contentInFocusModel.contentType else "Unknown"
            val c2_language = if (null != x.otherContentModel) x.contentInFocusModel.languageCode.mkString(",") else "Unknown"
            val psc = x.device_spec.primary_secondary_camera.split(",");
            val ps = if (psc.length == 0) {
                (0.0, 0.0);
            } else if (psc.length == 1) {
                (if (StringUtils.isBlank(psc(0))) 0.0 else psc(0).toDouble, 0.0);
            } else {
                (if (StringUtils.isBlank(psc(0))) 0.0 else psc(0).toDouble, if (StringUtils.isBlank(psc(1))) 0.0 else psc(1).toDouble);
            }
            val primary_camera = ps._1
            val secondary_camera = ps._2

            val dataMap = Map(
                "index" -> f._1,
                "did" -> x.did,
                "c1_content_id" -> x.contentInFocus,
                "c2_content_id" -> x.otherContentId,
                "c3_content_id" -> x.device_usage.last_played_content.getOrElse(""),
                "c1_text" -> c1_text,
                "c1_tag" -> c1_tag,
                "c1_total_ts" -> x.contentInFocusUsageSummary.total_timespent.getOrElse(0.0),
                "c1_subject" -> x.contentInFocusModel.subject.mkString(","),
                "c1_contentType" -> x.contentInFocusModel.contentType,
                "c1_language" -> x.contentInFocusModel.languageCode.mkString(","),
                "c1_publish_date" -> x.contentInFocusSummary.m_publish_date.getMillis,
                "c1_last_sync_date" -> x.contentInFocusSummary.m_last_sync_date.getMillis,
                "c1_total_timespent" -> x.contentInFocusSummary.m_total_ts,
                "c1_total_sessions" -> x.contentInFocusSummary.m_total_sessions,
                "c1_avg_ts_session" -> x.contentInFocusSummary.m_avg_ts_session,
                "c1_num_interactions" -> x.contentInFocusSummary.m_total_interactions,
                "c1_mean_interactions_min" -> x.contentInFocusSummary.m_avg_interactions_min,
                "c1_total_devices" -> x.contentInFocusSummary.m_total_devices,
                "c1_avg_sess_devices" -> x.contentInFocusSummary.m_avg_sess_device,
                "c2_text" -> c2_text,
                "c2_tag" -> c2_tag,
                "c2_total_ts" -> x.otherContentUsageSummary.total_timespent.getOrElse(0.0),
                "c2_avg_interactions_min" -> x.otherContentUsageSummary.avg_interactions_min.getOrElse(0.0),
                "c2_download_date" -> x.otherContentUsageSummary.download_date.getOrElse(0L),
                "c2_downloaded" -> c2_downloaded,
                "c2_last_played_on" -> x.otherContentUsageSummary.last_played_on.getOrElse(0L),
                "c2_mean_play_time_interval" -> x.otherContentUsageSummary.mean_play_time_interval.getOrElse(0.0),
                "c2_num_group_user" -> x.otherContentUsageSummary.num_group_user.getOrElse(0L),
                "c2_num_individual_user" -> x.otherContentUsageSummary.num_individual_user.getOrElse(0L),
                "c2_num_sessions" -> x.otherContentUsageSummary.num_sessions.getOrElse(0L),
                "c2_start_time" -> x.otherContentUsageSummary.start_time.getOrElse(0L),
                "c2_total_interactions" -> x.otherContentUsageSummary.total_interactions.getOrElse(0L),
                "c2_subject" -> c2_subject,
                "c2_contentType" -> c2_contentType,
                "c2_language" -> c2_language,
                "c2_publish_date" -> x.otherContentSummary.m_publish_date.getMillis,
                "c2_last_sync_date" -> x.otherContentSummary.m_last_sync_date.getMillis,
                "c2_total_timespent" -> x.otherContentSummary.m_total_ts,
                "c2_total_sessions" -> x.otherContentSummary.m_total_sessions,
                "c2_avg_ts_session" -> x.otherContentSummary.m_avg_ts_session,
                "c2_num_interactions" -> x.otherContentSummary.m_total_interactions,
                "c2_mean_interactions_min" -> x.otherContentSummary.m_avg_interactions_min,
                "c2_total_devices" -> x.otherContentSummary.m_total_devices,
                "c2_avg_sess_devices" -> x.otherContentSummary.m_avg_sess_device,
                "total_timespent" -> x.device_usage.total_timespent.getOrElse(0.0),
                "total_launches" -> x.device_usage.total_launches.getOrElse(0L),
                "total_play_time" -> x.device_usage.total_play_time.getOrElse(0.0),
                "avg_num_launches" -> x.device_usage.avg_num_launches.getOrElse(0.0),
                "avg_time" -> x.device_usage.avg_time.getOrElse(0.0),
                "end_time" -> x.device_usage.end_time.getOrElse(0L),
                "last_played_on" -> x.device_usage.last_played_on.getOrElse(0L),
                "mean_play_time" -> x.device_usage.mean_play_time.getOrElse(0.0),
                "mean_play_time_interval" -> x.device_usage.mean_play_time_interval.getOrElse(0.0),
                "num_contents" -> x.device_usage.num_contents.getOrElse(0L),
                "num_days" -> x.device_usage.num_days.getOrElse(0L),
                "num_sessions" -> x.device_usage.num_sessions.getOrElse(0L),
                "play_start_time" -> x.device_usage.play_start_time.getOrElse(0L),
                "start_time" -> x.device_usage.start_time.getOrElse(0L),
                "c3_text" -> c3_text,
                "c3_tag" -> c3_tag,
                "device_spec" -> x.device_spec.make,
                "screen_size" -> x.device_spec.screen_size,
                "external_disk" -> x.device_spec.external_disk,
                "internal_disk" -> x.device_spec.internal_disk,
                "primary_camera" -> primary_camera,
                "secondary_camera" -> secondary_camera)

            JSONUtils.serialize(dataMap)
        }
    }
}