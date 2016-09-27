package org.ekstep.analytics.model

import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer, RFormula }
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
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
import org.ekstep.analytics.updater.ContentUsageSummaryFact
import org.joda.time.DateTime
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.dispatcher.FileDispatcher
import org.ekstep.analytics.updater.DeviceSpec

case class DeviceMetrics(did: DeviceId, content_list: Map[String, ContentModel], device_usage: DeviceUsageSummary, device_spec: DeviceSpec, device_content: Map[String, DeviceContentSummary], dcT: Map[String, dcus_tf]);
case class DeviceContext(did: String, contentInFocus: String, contentInFocusModel: ContentModel, contentInFocusVec: ContentToVector, contentInFocusUsageSummary: DeviceContentSummary, contentInFocusSummary: ContentUsageSummaryFact, otherContentId: String, otherContentModel: ContentModel, otherContentModelVec: ContentToVector, otherContentUsageSummary: DeviceContentSummary, otherContentSummary: ContentUsageSummaryFact, device_usage: DeviceUsageSummary, device_spec: DeviceSpec, otherContentSummaryT: cus_t, dusT: dus_tf, dcusT: dcus_tf) extends AlgoInput;
case class DeviceRecos(device_id: String, scores: List[(String, Option[Double])]) extends AlgoOutput with Output
case class ContentToVector(contentId: String, text_vec: Option[List[Double]], tag_vec: Option[List[Double]]);

case class cus_t(c2_publish_date: Option[String], c2_last_sync_date: Option[String]);
case class dus_tf(end_time: Option[String], last_played_on: Option[String], play_start_time: Option[String], start_time: Option[String]);
case class dcus_tf(contentId: String, download_date: Option[String], last_played_on: Option[String], start_time: Option[String]);


object DeviceRecommendationModel extends IBatchModelTemplate[DerivedEvent, DeviceContext, DeviceRecos, DeviceRecos] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DeviceRecommendationModel"
    override def name(): String = "DeviceRecommendationModel"

    val defaultDCUS = DeviceContentSummary(null, null, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultDUS = DeviceUsageSummary(null, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultCUS = ContentUsageSummaryFact(0, null, null, new DateTime(0), new DateTime(0), new DateTime(0), 0.0, 0L, 0.0, 0L, 0.0);
    
    def choose[A](it: Buffer[A], r: Random): A = {
        val random_index = r.nextInt(it.size);
        it(random_index);
    }

    private def _getContentUsageTransformations(rdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): RDD[(String, cus_t)] = {
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.d_content_id, x.m_publish_date.getMillis.toDouble) };
        val f1_t = transformByBinning(f1, 3);
        val f2 = rdd.map { x => (x.d_content_id, x.m_last_sync_date.getMillis.toDouble) };
        val f2_t = transformByBinning(f2, 3);
        f1_t.join(f2_t).mapValues(f => cus_t(Option(f._1), Option(f._2)));
    }
    
    private def _getDeviceUsageTransformations(rdd: RDD[DeviceUsageSummary])(implicit sc: SparkContext): RDD[(String, dus_tf)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.device_id, x.end_time.getOrElse(0L).toDouble) };
        val f1_t = transformByBinning(f1, 3);
        val f2 = rdd.map { x => (x.device_id, x.last_played_on.getOrElse(0L).toDouble) };
        val f2_t = transformByBinning(f2, 3);
        val f3 = rdd.map { x => (x.device_id, x.play_start_time.getOrElse(0L).toDouble) };
        val f3_t = transformByBinning(f3, 3);
        val f4 = rdd.map { x => (x.device_id, x.start_time.getOrElse(0L).toDouble) };
        val f4_t = transformByBinning(f4, 3);
        f1_t.join(f2_t).join(f3_t).join(f4_t).mapValues(f => dus_tf(Option(f._1._1._1), Option(f._1._1._2), Option(f._1._2), Option(f._2)));
    }
    
    private def _getDeviceContentUsageTransformations(rdd: RDD[DeviceContentSummary])(implicit sc: SparkContext): RDD[(String, dcus_tf)] = {
        
        val indexedRDD = rdd.zipWithIndex().map { case (k, v) => (v, k) }.map(x => (x._1.toString(), x._2))
        implicit val sqlContext = new SQLContext(sc);
        val f1 = indexedRDD.map { x => (x._1, x._2.download_date.getOrElse(0L).toDouble) };
        val f1_t = transformByBinning(f1, 3);
        val f2 = indexedRDD.map { x => (x._1, x._2.last_played_on.getOrElse(0L).toDouble) };
        val f2_t = transformByBinning(f2, 3);
        val f3 = indexedRDD.map { x => (x._1, x._2.start_time.getOrElse(0L).toDouble) };
        val f3_t = transformByBinning(f3, 3);
        val x = f1_t.join(f2_t).join(f3_t)//.mapValues(f => dcus_tf(f._1._1, f._1._2, f._2));
        indexedRDD.leftOuterJoin(x).map(f => (f._2._1.device_id,dcus_tf(f._2._1.content_id,Option(f._2._2.get._1._1), Option(f._2._2.get._1._2), Option(f._2._2.get._2))));
    }
    
    def transformByBinning(rdd: RDD[(String, Double)], numBuckets: Int)(implicit sqlContext: SQLContext): RDD[(String, String)] = {

        val rows = rdd.map(f => Row.fromSeq(Seq(f._1, f._2)));
        val structs = new StructType(Array(new StructField("key", StringType, true), new StructField("value", DoubleType, true)));
        val df = sqlContext.createDataFrame(rows, structs);
        val discretizer = new QuantileDiscretizer()
            .setInputCol("value")
            .setOutputCol("n_value")
            .setNumBuckets(numBuckets)

        val result = discretizer.fit(df).transform(df).drop("value").rdd;
        result.map { x => (x.getString(0), x.getDouble(1).toString())};
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContext] = {

        val limit = config.getOrElse("live_content_limit", 1000).asInstanceOf[Int];
        val contentModel = ContentAdapter.getLiveContent(limit).map { x => (x.id, x) }.toMap;
        JobLogger.log("Live content count", Option(Map("count" -> contentModel.size)), INFO);

        val contentModelB = sc.broadcast(contentModel);
        val contentVectors = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC).map { x => (x.contentId, x) }.collect().toMap;
        val contentVectorsB = sc.broadcast(contentVectors);
        
        // Content Usage Summaries
        val contentUsageSummaries = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period=? and d_tag = 'all'", 0).cache();
        val contentUsageTransformations = _getContentUsageTransformations(contentUsageSummaries).map { x => (x._1, x._2) }.collect().toMap;
        val contentUsageTB = sc.broadcast(contentUsageTransformations);
        val contentUsage = contentUsageSummaries.map { x => (x.d_content_id, x) }.collect().toMap;
        val contentUsageB = sc.broadcast(contentUsage);
        contentUsageSummaries.unpersist(true);
        
        val device_spec = sc.cassandraTable[DeviceSpec](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE).map { x => (DeviceId(x.device_id), x) }
        val allDevices = device_spec.map(x => x._1).distinct; // TODO: Do we need distinct here???
        
        // Device Usage Summaries data transformations
        val dus = sc.cassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).cache();
        val dusTransformations = _getDeviceUsageTransformations(dus).map { x => (x._1, x._2) }.collect().toMap;
        val dusTB = sc.broadcast(dusTransformations);
        dus.unpersist(true);
        
        val device_usage = allDevices.joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => (x._1, x._2) }

        val dcus = allDevices.joinWithCassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).cache();
        val device_content_usage = dcus.groupBy(f => f._1).mapValues(f => f.map(x => x._2));
        val dcusTransformations = _getDeviceContentUsageTransformations(dcus.map(x => x._2)).map { x => (x._1, x._2) }.groupBy(f => f._1).mapValues(f => f.map(x => x._2)).collect().toMap;
        val dcusTB = sc.broadcast(dcusTransformations);
        dcus.unpersist(true);
        
        device_spec.leftOuterJoin(device_usage).leftOuterJoin(device_content_usage).map { x =>
            val dc = x._2._2.getOrElse(Buffer[DeviceContentSummary]()).map { x => (x.content_id, x) }.toMap;
            val dcT = dcusTB.value.getOrElse(x._1.device_id, Buffer[dcus_tf]()).map { x => (x.contentId, x) }.toMap;
            DeviceMetrics(x._1, contentModelB.value, x._2._1._2.getOrElse(defaultDUS), x._2._1._1, dc, dcT)
        }.map { x =>
            val rand = new Random(System.currentTimeMillis());
            x.content_list.map { y =>
                {
                    val randomContent: DeviceContentSummary = if (x.device_content.isEmpty) defaultDCUS else choose[(String, DeviceContentSummary)](x.device_content.toBuffer, rand)._2;
                    val dcusT: dcus_tf = if (x.dcT.isEmpty) dcus_tf(null, None, None, None) else choose[(String, dcus_tf)](x.dcT.toBuffer, rand)._2;
                    val otherContentId: String = randomContent.content_id;
                    val otherContentModel: ContentModel = if (null != otherContentId) contentModelB.value.getOrElse(otherContentId, null) else null;
                    val contentInFocusVec = contentVectorsB.value.getOrElse(y._1, null);
                    val otherContentModelVec = if (null != otherContentId) contentVectorsB.value.getOrElse(randomContent.content_id, null) else null;
                    val contentInFocusUsageSummary = x.device_content.getOrElse(y._1, defaultDCUS);
                    val contentInFocusSummary = contentUsageB.value.getOrElse(y._1, defaultCUS)
                    val otherContentSummary = if (null != otherContentId) contentUsageB.value.getOrElse(otherContentId, defaultCUS) else defaultCUS;
                    val otherContentSummaryT = if (null != otherContentId) contentUsageTB.value.getOrElse(otherContentId, cus_t(None,None)) else cus_t(None,None);
                    val dusT = dusTB.value.getOrElse(x.did.device_id, dus_tf(None, None, None, None));
                    DeviceContext(x.did.device_id, y._1, y._2, contentInFocusVec, contentInFocusUsageSummary, contentInFocusSummary, otherContentId, otherContentModel, otherContentModelVec, randomContent, otherContentSummary, x.device_usage, x.device_spec, otherContentSummaryT, dusT, dcusT);
                }
            }
        }.flatMap { x => x.map { f => f } }

    }

    private def _createDF(data: RDD[DeviceContext]): RDD[Row] = {
        data.map { x =>
            val seq = ListBuffer[Any]();
            //seq += x.did; // did
            // Add c1 text vectors
            seq ++= (if (null != x.contentInFocusVec && x.contentInFocusVec.text_vec.isDefined) {
                x.contentInFocusVec.text_vec.get.toSeq
            } else {
                _getZeros(50);
            })
            // Add c1 tag vectors
            seq ++= (if (null != x.contentInFocusVec && x.contentInFocusVec.tag_vec.isDefined) {
                x.contentInFocusVec.tag_vec.get.toSeq
            } else {
                _getZeros(50);
            })
            // Add c1 context attributes
            seq ++= Seq(x.contentInFocusUsageSummary.total_timespent.getOrElse(0.0))
            //            seq ++= Seq(x.contentInFocusUsageSummary.total_timespent.getOrElse(0.0), x.contentInFocusUsageSummary.avg_interactions_min.getOrElse(0.0),
            //                x.contentInFocusUsageSummary.download_date.getOrElse(0L), if (x.contentInFocusUsageSummary.downloaded.getOrElse(false)) 1 else 0, x.contentInFocusUsageSummary.last_played_on.getOrElse(0L),
            //                x.contentInFocusUsageSummary.mean_play_time_interval.getOrElse(0.0), x.contentInFocusUsageSummary.num_group_user.getOrElse(0L), x.contentInFocusUsageSummary.num_individual_user.getOrElse(0L),
            //                x.contentInFocusUsageSummary.num_sessions.getOrElse(0L), x.contentInFocusUsageSummary.start_time.getOrElse(0L), x.contentInFocusUsageSummary.total_interactions.getOrElse(0L))
            seq ++= Seq(x.contentInFocusModel.subject.mkString(","), x.contentInFocusModel.contentType, x.contentInFocusModel.languageCode.mkString(","))

            // Add c1 usage metrics
            seq ++= Seq(x.contentInFocusSummary.m_publish_date.getMillis, x.contentInFocusSummary.m_last_sync_date.getMillis, x.contentInFocusSummary.m_total_ts,
                x.contentInFocusSummary.m_total_sessions, x.contentInFocusSummary.m_avg_ts_session, x.contentInFocusSummary.m_total_interactions, x.contentInFocusSummary.m_avg_interactions_min)

            // Add c2 text vectors
            seq ++= (if (null != x.otherContentModelVec && x.otherContentModelVec.text_vec.isDefined) {
                x.otherContentModelVec.text_vec.get.toSeq
            } else {
                _getZeros(50);
            })
            // Add c2 tag vectors
            seq ++= (if (null != x.otherContentModelVec && x.otherContentModelVec.tag_vec.isDefined) {
                x.otherContentModelVec.tag_vec.get.toSeq
            } else {
                _getZeros(50);
            })

            // Add c2 context attributes
            seq ++= Seq(x.otherContentUsageSummary.total_timespent.getOrElse(0.0), x.otherContentUsageSummary.avg_interactions_min.getOrElse(0.0),
                x.dcusT.download_date.getOrElse("Unknown"), if (x.otherContentUsageSummary.downloaded.getOrElse(false)) 1 else 0, x.dcusT.last_played_on.getOrElse("Unknown"),
                x.otherContentUsageSummary.mean_play_time_interval.getOrElse(0.0), x.otherContentUsageSummary.num_group_user.getOrElse(0L), x.otherContentUsageSummary.num_individual_user.getOrElse(0L),
                x.otherContentUsageSummary.num_sessions.getOrElse(0L), x.dcusT.start_time.getOrElse("Unknown"), x.otherContentUsageSummary.total_interactions.getOrElse(0L))

            seq ++= (if (null != x.otherContentModel) {
                Seq(x.otherContentModel.subject.mkString(","), x.contentInFocusModel.contentType, x.contentInFocusModel.languageCode.mkString(","))
            } else {
                Seq("Unknown", "Unknown", "Unknown")
            })

            // Add c2 usage metrics
            seq ++= Seq(x.otherContentSummaryT.c2_publish_date.getOrElse("Unknown"), x.otherContentSummaryT.c2_last_sync_date.getOrElse("Unknown"), x.otherContentSummary.m_total_ts,
                x.otherContentSummary.m_total_sessions, x.otherContentSummary.m_avg_ts_session, x.otherContentSummary.m_total_interactions, x.otherContentSummary.m_avg_interactions_min)

            //println(x.did, "x.device_usage.total_launches", x.device_usage.total_launches, x.device_usage.total_launches.getOrElse(0L).isInstanceOf[Long]);
            // TODO: x.device_usage.total_launches is being considered as Double - Debug further
            // Device Context Attributes
            seq ++= Seq(x.device_usage.total_timespent.getOrElse(0.0), x.device_usage.total_launches.getOrElse(0L), x.device_usage.total_play_time.getOrElse(0.0),
                x.device_usage.avg_num_launches.getOrElse(0.0), x.device_usage.avg_time.getOrElse(0.0), x.dusT.end_time.getOrElse("Unknown"),
                x.dusT.last_played_on.getOrElse("Unknown"), x.device_usage.mean_play_time.getOrElse(0.0), x.device_usage.mean_play_time_interval.getOrElse(0.0),
                x.device_usage.num_contents.getOrElse(0L), x.device_usage.num_days.getOrElse(0L), x.device_usage.num_sessions.getOrElse(0L), x.dusT.play_start_time.getOrElse("Unknown"),
                x.dusT.start_time.getOrElse("Unknown"))
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

    private def _getStructType(): StructType = {
        val seq = ListBuffer[StructField]();
        //seq += new StructField("did", StringType, false); // did
        // Add c1 text vectors
        seq ++= _getStructField("c1_text", 50);
        // Add c1 tag vectors
        seq ++= _getStructField("c1_tag", 50);
        // Add c1 context attributes
        seq ++= Seq(new StructField("c1_total_ts", DoubleType, true))
        //        seq ++= Seq(new StructField("c1_total_ts", DoubleType, true), new StructField("c1_avg_interactions_min", DoubleType, true), new StructField("c1_download_date", LongType, true),
        //            new StructField("c1_downloaded", IntegerType, true), new StructField("c1_last_played_on", LongType, true), new StructField("c1_mean_play_time_interval", DoubleType, true),
        //            new StructField("c1_num_group_user", LongType, true), new StructField("c1_num_individual_user", LongType, true), new StructField("c1_num_sessions", LongType, true),
        //            new StructField("c1_start_time", LongType, true), new StructField("c1_total_interactions", LongType, true));
        seq ++= Seq(new StructField("c1_subject", StringType, true), new StructField("c1_contentType", StringType, true), new StructField("c1_language", StringType, true));

        // Add c1 usage metrics
        seq ++= Seq(new StructField("c1_publish_date", LongType, true),
            new StructField("c1_last_sync_date", LongType, true), new StructField("c1_total_timespent", DoubleType, true), new StructField("c1_total_sessions", LongType, true),
            new StructField("c1_avg_ts_session", DoubleType, true), new StructField("c1_num_interactions", LongType, true), new StructField("c1_mean_interactions_min", DoubleType, true));

        // Add c2 text vectors
        seq ++= _getStructField("c2_text", 50);
        // Add c2 tag vectors
        seq ++= _getStructField("c2_tag", 50);
        // Add c2 context attributes

        seq ++= Seq(new StructField("c2_total_ts", DoubleType, true), new StructField("c2_avg_interactions_min", DoubleType, true), new StructField("c2_download_date", StringType, true),
            new StructField("c2_downloaded", IntegerType, true), new StructField("c2_last_played_on", StringType, true), new StructField("c2_mean_play_time_interval", DoubleType, true),
            new StructField("c2_num_group_user", LongType, true), new StructField("c2_num_individual_user", LongType, true), new StructField("c2_num_sessions", LongType, true),
            new StructField("c2_start_time", StringType, true), new StructField("c2_total_interactions", LongType, true))

        seq ++= Seq(new StructField("c2_subject", StringType, true), new StructField("c2_contentType", StringType, true), new StructField("c2_language", StringType, true))

        // Add c2 usage metrics
        seq ++= Seq(new StructField("c2_publish_date", StringType, true),
            new StructField("c2_last_sync_date", StringType, true), new StructField("c2_total_timespent", DoubleType, true), new StructField("c2_total_sessions", LongType, true),
            new StructField("c2_avg_ts_session", DoubleType, true), new StructField("c2_num_interactions", LongType, true), new StructField("c2_mean_interactions_min", DoubleType, true));

        // Device Context Attributes
        seq ++= Seq(new StructField("total_timespent", DoubleType, true), new StructField("total_launches", LongType, true), new StructField("total_play_time", DoubleType, true),
            new StructField("avg_num_launches", DoubleType, true), new StructField("avg_time", DoubleType, true), new StructField("end_time", StringType, true),
            new StructField("last_played_on", StringType, true), new StructField("mean_play_time", DoubleType, true), new StructField("mean_play_time_interval", DoubleType, true),
            new StructField("num_contents", LongType, true), new StructField("num_days", LongType, true), new StructField("num_sessions", LongType, true),
            new StructField("play_start_time", StringType, true), new StructField("start_time", StringType, true))

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

    override def algorithm(data: RDD[DeviceContext], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceRecos] = {

        JobLogger.log("Running the algorithm", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        implicit val sqlContext = new SQLContext(sc);
        val libfmFile = config.getOrElse("dataFile", "/tmp/score.dat.libfm").asInstanceOf[String]
        val trainDataFile = config.getOrElse("trainDataFile", "/tmp/train.dat.libfm").asInstanceOf[String]
        //        val testDataFile = config.getOrElse("testDataFile", "/tmp/test.dat.libfm").asInstanceOf[String]
        val outputFile = config.getOrElse("outputFile", "/tmp/score.txt").asInstanceOf[String]
        val libfmInputFile = config.getOrElse("libfmInputFile", "/tmp/libfm_input.csv").asInstanceOf[String]
        val model = config.getOrElse("model", "/tmp/fm.model").asInstanceOf[String]
        val libfmLogFile = config.getOrElse("libfmLogFile", "/tmp/logFile").asInstanceOf[String]
        val libfmExec = config.getOrElse("libfm.executable_path", "/usr/local/bin/") + "libFM";
        val bucket = config.getOrElse("bucket", "sandbox-data-store").asInstanceOf[String];
        val key = config.getOrElse("key", "model/fm.model").asInstanceOf[String];
        //        val libFMTrainConfig = config.getOrElse("libFMTrainConfig", "-dim 1,10,175 -iter 100 -method mcmc -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 0.1")
        val libFMScoreConfig = config.getOrElse("libFMScoreConfig", "-dim 1,1,0 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")

        CommonUtil.deleteFile(libfmFile);
        CommonUtil.deleteFile(trainDataFile);
        //        CommonUtil.deleteFile(testDataFile);
        CommonUtil.deleteFile(outputFile);
        CommonUtil.deleteFile(model);
        CommonUtil.deleteFile(libfmInputFile);
        CommonUtil.deleteFile(libfmLogFile);

        JobLogger.log("Creating dataframe and libfm data", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        val rdd: RDD[Row] = _createDF(data);
        JobLogger.log("Creating RDD[Row]", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        val df = sqlContext.createDataFrame(rdd, _getStructType);
        JobLogger.log("Created dataframe and libfm data", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);

        // c2_publish_date, c2_last_sync_date - CUS
        // c2_download_date, c2_last_played_on, c2_start_time - DCUS 
        // end_time, last_played_on, mean_play_time, play_start_time - DUS
        
        /*
        val columns = Array("c2_download_date", "c2_last_played_on", "c2_start_time", "c2_publish_date", "c2_last_sync_date", "end_time", "last_played_on", "mean_play_time", "play_start_time")
        JobLogger.log("calling binning method", None, INFO);
        val resultDF = binning(df, columns, 3)
        JobLogger.log("completed binning operation", None, INFO);
        *
        */
        
        if (config.getOrElse("saveDataFrame", false).asInstanceOf[Boolean]) {
            val columnNames = df.columns.mkString(",")
            val rdd = df.map { x => x.mkString(",") }.persist(StorageLevel.MEMORY_AND_DISK)
            JobLogger.log("save DF to libfmInputFile", None, INFO);
            OutputDispatcher.dispatchDF(Dispatcher("file", Map("file" -> libfmInputFile)), rdd, columnNames);
        }
        JobLogger.log("save DF to libfmInputFile completed", None, INFO);
        
        val formula = new RFormula()
            .setFormula("c1_total_ts ~ .")
            .setFeaturesCol("features")
            .setLabelCol("label")
        JobLogger.log("applied the formula", None, INFO);    
        val output = formula.fit(df).transform(df)
        JobLogger.log("executing formula.fit(resultDF).transform(resultDF)", None, INFO);
        
        val labeledRDD = output.select("features", "label").map { x => new LabeledPoint(x.getDouble(1), x.getAs[Vector](0)) };
        JobLogger.log("created labeledRDD", None, INFO);
        
        val dataStr = labeledRDD.map {
            case LabeledPoint(label, features) =>

                val sb = new StringBuilder(label.toString)
                val sv = features.toSparse;
                val indices = sv.indices;
                val values = sv.values;
                sb += ' '
                sb ++= (0 until indices.length).map { x =>
                    (indices(x) + 1).toString() + ":" + values(x).toString()
                }.toSeq.mkString(" ");

                sb.mkString
        }
        

        
        JobLogger.log("Creating training dataset", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        //val usedDataSet = dataStr.filter { x => !StringUtils.startsWith(x, "0.0") }
        
        
        //OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> trainDataFile)), usedDataSet);
        
        
        //JobLogger.log("Creating scoring dataset", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        //OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> libfmFile)), dataStr);

        FileDispatcher.dispatchRETrainingFile(Map("file1" -> trainDataFile, "file2" -> libfmFile), dataStr);
        
        JobLogger.log("Running the scoring algorithm", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        ScriptDispatcher.dispatch(Array(), Map("script" -> s"$libfmExec -train $trainDataFile -test $libfmFile $libFMScoreConfig -out $outputFile -save_model $model", "PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin")));

        JobLogger.log("Save the model to S3", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        S3Dispatcher.dispatch(null, Map("filePath" -> model, "bucket" -> bucket, "key" -> key))

        JobLogger.log("Load the scores into memory and join with device content", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        val device_content = data.map { x => (x.did, x.contentInFocus) }.zipWithIndex()
        val dcWithIndexKey = device_content.map { case (k, v) => (v, k) }
        val scores = sc.textFile(outputFile).map { x => x.toDouble }.zipWithIndex()
        val scoresWithIndexKey = scores.map { case (k, v) => (v, k) }
        val device_scores = dcWithIndexKey.leftOuterJoin(scoresWithIndexKey).map { x => x._2 }.groupBy(x => x._1._1).mapValues(f => f.map(x => (x._1._2, x._2)).toList.sortBy(y => y._2).reverse)
        device_scores.map { x =>
            DeviceRecos(x._1, x._2)
        }
    }

    override def postProcess(data: RDD[DeviceRecos], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceRecos] = {

        JobLogger.log("Save the scores to cassandra", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        data.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_RECOS)
        data;
    }

}
