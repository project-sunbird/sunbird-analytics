package org.ekstep.analytics.model

import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer, RFormula }
import org.apache.spark.ml.feature.QuantileDiscretizer
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
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.adapter.ContentResponse
import org.ekstep.analytics.framework.conf.AppConf

object DeviceRecommendationScoringModel extends IBatchModelTemplate[DerivedEvent, DeviceContext, DeviceRecos, DeviceRecos] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DeviceRecommendationScoringModel"
    override def name(): String = "DeviceRecommendationScoringModel"

    val defaultDCUS = DeviceContentSummary(null, null, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultDUS = DeviceUsageSummary(null, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultCUS = ContentUsageSummaryFact(0, null, null, new DateTime(0), new DateTime(0), new DateTime(0), 0.0, 0L, 0.0, 0L, 0.0, 0, 0.0, null);
    val dateTime = new DateTime()
    val date = dateTime.toLocalDate()
    val time = dateTime.toLocalTime().toString("hh-mm")
    val path = "/scoring/" + date + "/" + time + "/"
    
    def choose[A](it: Buffer[A], r: Random): A = {
        val random_index = r.nextInt(it.size);
        it(random_index);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContext] = {

        val limit = config.getOrElse("live_content_limit", 1000).asInstanceOf[Int];
        val num_bins = config.getOrElse("num_bins", 4).asInstanceOf[Int];
        
        // Content Usage Summaries
        val contentUsageSummaries = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period=? and d_tag = 'all'", 0).map { x => x }.cache();
        // cus transformations
        val cusT = ContentUsageTransformer.getTransformationByBinning(contentUsageSummaries, num_bins)
        val contentUsageB = cusT.map { x => (x._1, x._2) }.collect().toMap
        val contentUsageBB = sc.broadcast(contentUsageB);
        val contentUsageO = contentUsageSummaries.map{x => (x.d_content_id, x)}.collect().toMap
        val contentUsageOB = sc.broadcast(contentUsageO);
        contentUsageSummaries.unpersist(true);

         //Content Model
        val baseUrl = AppConf.getConfig("service.search.url");
        val searchUrl = s"$baseUrl/v2/search";
        val contentIds = contentUsageSummaries.map{x => x.d_content_id}.distinct.collect().toList;
        val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "identifier" -> contentIds), "exists" -> List("downloadUrl"), "limit" -> contentIds.size));
        val resp = RestUtil.post[ContentResponse](searchUrl, JSONUtils.serialize(request));
        val cusContentModel = resp.result.content.map(f => ContentModel(f.getOrElse("identifier", "").asInstanceOf[String], f.getOrElse("domain", List("literacy")).asInstanceOf[List[String]], f.getOrElse("contentType", "").asInstanceOf[String], f.getOrElse("language", List[String]()).asInstanceOf[List[String]]))//.map { x => (x.id, x) }.toMap;
        val liveContentModel = ContentAdapter.getLiveContent(limit)//.map { x => (x.id, x) }.toMap;
        val contentModel = (cusContentModel ++ liveContentModel).distinct.map { x => (x.id, x) }.toMap;
        JobLogger.log("Content count", Option(Map("count" -> contentModel.size)), INFO, "org.ekstep.analytics.model");
        val contentModelB = sc.broadcast(contentModel);
        val contentVectors = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC).map { x => (x.contentId, x) }.collect().toMap;
        val contentVectorsB = sc.broadcast(contentVectors);

        // device-specifications
        val device_spec = sc.cassandraTable[DeviceSpec](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE).map { x => (DeviceId(x.device_id), x) }
        val allDevices = device_spec.map(x => x._1).distinct; // TODO: Do we need distinct here???

        // Device Usage Summaries
        val device_usage = allDevices.joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => x._2 } //.map { x => (x._1, x._2) }
        // dus transformations
        val dusT = DeviceUsageTransformer.getTransformationByBinning(device_usage, num_bins)
        val dusB = dusT.map { x => (x._1, x._2) }.collect().toMap;
        val dusBB = sc.broadcast(dusB);
        val dusO = device_usage.map { x => (DeviceId(x.device_id), x) }

        // Device Content Usage Summaries
        val dcus = sc.cassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).cache();
        // dcus transformations
        val dcusT = DeviceContentUsageTransformer.getTransformationByBinning(dcus, num_bins)
        val dcusB = dcusT.map { x => (x._1, x._2) }.groupBy(f => f._1).mapValues(f => f.map(x => x._2)).collect().toMap;
        val dcusBB = sc.broadcast(dcusB);
        val dcusO = dcus.map { x => (DeviceId(x.device_id), x) }.groupBy(f => f._1).mapValues(f => f.map(x => x._2));

        dcus.unpersist(true);
        
        device_spec.leftOuterJoin(dusO).leftOuterJoin(dcusO).map { x =>
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

    def scoringAlgo(data: RDD[org.apache.spark.ml.linalg.DenseVector], localPath: String, model: String )(implicit sc: SparkContext): RDD[Double] ={
        JobLogger.log(localPath + model, None ,INFO, "org.ekstep.analytics.model")
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
        
        JobLogger.log("Running the algorithm", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        implicit val sqlContext = new SQLContext(sc);

        val localPath = config.getOrElse("localPath", "/tmp/").asInstanceOf[String]
        val outputFile = localPath + path + "score.txt"
        val model_name = config.getOrElse("model_name", "fm.model").asInstanceOf[String]
        val bucket = config.getOrElse("bucket", "sandbox-data-store").asInstanceOf[String];
        val key = config.getOrElse("key", "model/").asInstanceOf[String];
        val tag_dimensions = config.getOrElse("tag_dimensions", 50).asInstanceOf[Int];
        val text_dimensions = config.getOrElse("text_dimensions", 50).asInstanceOf[Int];
        val upload_score_s3 = config.getOrElse("upload_score_s3", true).asInstanceOf[Boolean];
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

        val featureVector = output.select("features").rdd.map { x => x.getAs[org.apache.spark.ml.linalg.SparseVector](0).toDense };//x.asInstanceOf[org.apache.spark.mllib.linalg.DenseVector] };
        JobLogger.log("created featureVector", None, INFO, "org.ekstep.analytics.model");
        
        JobLogger.log("Downloading model from s3", None, INFO, "org.ekstep.analytics.model");
        S3Util.downloadFile(bucket, key + model_name, localPath + path)
        
        JobLogger.log("Running scoring algorithm", None, INFO, "org.ekstep.analytics.model");
        val scores = scoringAlgo(featureVector, localPath + path, model_name)

        JobLogger.log("Dispatching scores to a file", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> outputFile)), scores);

        if(upload_score_s3)
        {
            JobLogger.log("Saving the score file to S3", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
            S3Dispatcher.dispatch(null, Map("filePath" -> outputFile, "bucket" -> bucket, "key" -> (key + "score.txt")))
        }
        
        JobLogger.log("Load the scores into memory and join with device content", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        val device_content = data.map { x => (x.did, x.contentInFocus) }.zipWithIndex().map { case (k, v) => (v, k) }
        val scoresIndexed = scores.zipWithIndex().map { case (k, v) => (v, k) }
        val device_scores = device_content.leftOuterJoin(scoresIndexed).map { x => x._2 }.groupBy(x => x._1._1).mapValues(f => f.map(x => (x._1._2, x._2)).toList.sortBy(y => y._2).reverse)
        device_scores.map { x =>
            DeviceRecos(x._1, x._2)
        }
    }

    override def postProcess(data: RDD[DeviceRecos], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceRecos] = {

        JobLogger.log("Save the scores to cassandra", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO, "org.ekstep.analytics.model");
        data.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_RECOS)
        data;
    }
}