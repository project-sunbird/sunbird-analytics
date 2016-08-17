package org.ekstep.analytics.model

import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer, RFormula }
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

case class DeviceMetrics(did: DeviceId, content_list: Map[String, ContentModel], device_usage: DeviceUsageSummary, device_spec: DeviceSpec, device_content: Map[String, DeviceContentSummary]);
case class DeviceContext(did: String, contentInFocus: String, contentInFocusModel: ContentModel, contentInFocusVec: ContentToVector, contentInFocusUsageSummary: DeviceContentSummary, contentInFocusSummary: ContentUsageSummaryFact, otherContentId: String, otherContentModel: ContentModel, otherContentModelVec: ContentToVector, otherContentUsageSummary: DeviceContentSummary, otherContentSummary: ContentUsageSummaryFact, device_usage: DeviceUsageSummary, device_spec: DeviceSpec) extends AlgoInput;
case class DeviceRecos(device_id: String, scores: List[(String, Option[Double])]) extends AlgoOutput with Output
case class ContentToVector(contentId: String, text_vec: Option[List[Double]], tag_vec: Option[List[Double]]);

object DeviceRecommendationModel extends IBatchModelTemplate[DerivedEvent, DeviceContext, DeviceRecos, DeviceRecos] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DeviceRecommendationEngine"
    override def name(): String = "DeviceRecommendationEngine"

    val defaultDCUS = DeviceContentSummary(null, null, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultDUS = DeviceUsageSummary(null, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultCUS = ContentUsageSummaryFact(null, 0, false, null, "Unknown", new DateTime(0), new DateTime(0), 0.0, 0L, 0.0, 0L, 0.0, None, None)

    def choose[A](it: Buffer[A], r: Random): A = {
        val random_index = r.nextInt(it.size);
        it(random_index);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContext] = {

        val contentModel = ContentAdapter.getLiveContent().map { x => (x.id, x) }.toMap;
        JobLogger.log("Live content count", Option(Map("count" -> contentModel.size)), INFO);

        val contentModelB = sc.broadcast(contentModel);
        val contentVectors = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC).map { x => (x.contentId, x) }.collect().toMap;
        val contentVectorsB = sc.broadcast(contentVectors);
        val contentUsage = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period=?", 0).map { x => (x.d_content_id, x) }.collect().toMap;
        val contentUsageB = sc.broadcast(contentUsage);
        val device_spec = sc.cassandraTable[DeviceSpec](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE).map { x => (DeviceId(x.device_id), x) }
        val allDevices = device_spec.map(x => x._1).distinct; // TODO: Do we need distinct here???
        val device_usage = allDevices.joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => (x._1, x._2) }

        val device_content_usage = allDevices.joinWithCassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).groupBy(f => f._1).mapValues(f => f.map(x => x._2));

        device_spec.leftOuterJoin(device_usage).leftOuterJoin(device_content_usage).map { x =>
            val dc = x._2._2.getOrElse(Buffer[DeviceContentSummary]()).map { x => (x.content_id, x) }.toMap;
            DeviceMetrics(x._1, contentModelB.value, x._2._1._2.getOrElse(defaultDUS), x._2._1._1, dc)
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
                    DeviceContext(x.did.device_id, y._1, y._2, contentInFocusVec, contentInFocusUsageSummary, contentInFocusSummary, otherContentId, otherContentModel, otherContentModelVec, randomContent, otherContentSummary, x.device_usage, x.device_spec);
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
            seq ++= Seq(x.contentInFocusUsageSummary.total_timespent.getOrElse(0.0), x.contentInFocusUsageSummary.avg_interactions_min.getOrElse(0.0),
                x.contentInFocusUsageSummary.download_date.getOrElse(0L), if (x.contentInFocusUsageSummary.downloaded.getOrElse(false)) 1 else 0, x.contentInFocusUsageSummary.last_played_on.getOrElse(0L),
                x.contentInFocusUsageSummary.mean_play_time_interval.getOrElse(0.0), x.contentInFocusUsageSummary.num_group_user.getOrElse(0L), x.contentInFocusUsageSummary.num_individual_user.getOrElse(0L),
                x.contentInFocusUsageSummary.num_sessions.getOrElse(0L), x.contentInFocusUsageSummary.start_time.getOrElse(0L), x.contentInFocusUsageSummary.total_interactions.getOrElse(0L))
            seq ++= Seq(x.contentInFocusModel.subject.mkString(","), x.contentInFocusModel.contentType, x.contentInFocusModel.languageCode.mkString(","))

            // Add c1 usage metrics
            seq ++= Seq(if (x.contentInFocusSummary.d_group_user) 1 else 0, x.contentInFocusSummary.d_mime_type, x.contentInFocusSummary.m_publish_date.getMillis, x.contentInFocusSummary.m_last_sync_date.getMillis, x.contentInFocusSummary.m_total_ts,
                x.contentInFocusSummary.m_total_sessions, x.contentInFocusSummary.m_avg_ts_session, x.contentInFocusSummary.m_total_interactions, x.contentInFocusSummary.m_avg_interactions_min,
                x.contentInFocusSummary.m_avg_sessions_week.getOrElse(0.0), x.contentInFocusSummary.m_avg_ts_week.getOrElse(0.0))

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
                x.otherContentUsageSummary.download_date.getOrElse(0L), if (x.otherContentUsageSummary.downloaded.getOrElse(false)) 1 else 0, x.otherContentUsageSummary.last_played_on.getOrElse(0L),
                x.otherContentUsageSummary.mean_play_time_interval.getOrElse(0.0), x.otherContentUsageSummary.num_group_user.getOrElse(0L), x.otherContentUsageSummary.num_individual_user.getOrElse(0L),
                x.otherContentUsageSummary.num_sessions.getOrElse(0L), x.otherContentUsageSummary.start_time.getOrElse(0L), x.otherContentUsageSummary.total_interactions.getOrElse(0L))

            seq ++= (if (null != x.otherContentModel) {
                Seq(x.otherContentModel.subject.mkString(","), x.contentInFocusModel.contentType, x.contentInFocusModel.languageCode.mkString(","))
            } else {
                Seq("Unknown", "Unknown", "Unknown")
            })

            // Add c2 usage metrics
            seq ++= Seq(if (x.otherContentSummary.d_group_user) 1 else 0, x.otherContentSummary.d_mime_type, x.otherContentSummary.m_publish_date.getMillis, x.otherContentSummary.m_last_sync_date.getMillis, x.otherContentSummary.m_total_ts,
                x.otherContentSummary.m_total_sessions, x.otherContentSummary.m_avg_ts_session, x.otherContentSummary.m_total_interactions, x.otherContentSummary.m_avg_interactions_min,
                x.otherContentSummary.m_avg_sessions_week.getOrElse(0.0), x.otherContentSummary.m_avg_ts_week.getOrElse(0.0))

            //println(x.did, "x.device_usage.total_launches", x.device_usage.total_launches, x.device_usage.total_launches.getOrElse(0L).isInstanceOf[Long]);
            // TODO: x.device_usage.total_launches is being considered as Double - Debug further
            // Device Context Attributes
            seq ++= Seq(x.device_usage.total_timespent.getOrElse(0.0), x.device_usage.total_launches.getOrElse(0L), x.device_usage.total_play_time.getOrElse(0.0),
                x.device_usage.avg_num_launches.getOrElse(0.0), x.device_usage.avg_time.getOrElse(0.0), x.device_usage.end_time.getOrElse(0L),
                x.device_usage.last_played_on.getOrElse(0L), x.device_usage.mean_play_time.getOrElse(0.0), x.device_usage.mean_play_time_interval.getOrElse(0.0),
                x.device_usage.num_contents.getOrElse(0L), x.device_usage.num_days.getOrElse(0L), x.device_usage.num_sessions.getOrElse(0L), x.device_usage.play_start_time.getOrElse(0L),
                x.device_usage.start_time.getOrElse(0L))
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
        seq ++= Seq(new StructField("c1_total_ts", DoubleType, true), new StructField("c1_avg_interactions_min", DoubleType, true), new StructField("c1_download_date", LongType, true),
            new StructField("c1_downloaded", IntegerType, true), new StructField("c1_last_played_on", LongType, true), new StructField("c1_mean_play_time_interval", DoubleType, true),
            new StructField("c1_num_group_user", LongType, true), new StructField("c1_num_individual_user", LongType, true), new StructField("c1_num_sessions", LongType, true),
            new StructField("c1_start_time", LongType, true), new StructField("c1_total_interactions", LongType, true));
        seq ++= Seq(new StructField("c1_subject", StringType, true), new StructField("c1_contentType", StringType, true), new StructField("c1_language", StringType, true));

        // Add c1 usage metrics
        seq ++= Seq(new StructField("c1_group_user", IntegerType, true), new StructField("c1_mime_type", StringType, true), new StructField("c1_publish_date", LongType, true),
            new StructField("c1_last_sync_date", LongType, true), new StructField("c1_total_timespent", DoubleType, true), new StructField("c1_total_sessions", LongType, true),
            new StructField("c1_avg_ts_session", DoubleType, true), new StructField("c1_num_interactions", LongType, true), new StructField("c1_mean_interactions_min", DoubleType, true),
            new StructField("c1_avg_sessions_week", DoubleType, true), new StructField("c1_avg_ts_week", DoubleType, true));

        // Add c2 text vectors
        seq ++= _getStructField("c2_text", 50);
        // Add c2 tag vectors
        seq ++= _getStructField("c2_tag", 50);
        // Add c2 context attributes

        seq ++= Seq(new StructField("c2_total_ts", DoubleType, true), new StructField("c2_avg_interactions_min", DoubleType, true), new StructField("c2_download_date", LongType, true),
            new StructField("c2_downloaded", IntegerType, true), new StructField("c2_last_played_on", LongType, true), new StructField("c2_mean_play_time_interval", DoubleType, true),
            new StructField("c2_num_group_user", LongType, true), new StructField("c2_num_individual_user", LongType, true), new StructField("c2_num_sessions", LongType, true),
            new StructField("c2_start_time", LongType, true), new StructField("c2_total_interactions", LongType, true))

        seq ++= Seq(new StructField("c2_subject", StringType, true), new StructField("c2_contentType", StringType, true), new StructField("c2_language", StringType, true))

        // Add c2 usage metrics
        seq ++= Seq(new StructField("c2_group_user", IntegerType, true), new StructField("c2_mime_type", StringType, true), new StructField("c2_publish_date", LongType, true),
            new StructField("c2_last_sync_date", LongType, true), new StructField("c2_total_timespent", DoubleType, true), new StructField("c2_total_sessions", LongType, true),
            new StructField("c2_avg_ts_session", DoubleType, true), new StructField("c2_num_interactions", LongType, true), new StructField("c2_mean_interactions_min", DoubleType, true),
            new StructField("c2_avg_sessions_week", DoubleType, true), new StructField("c2_avg_ts_week", DoubleType, true));

        // Device Context Attributes
        seq ++= Seq(new StructField("total_timespent", DoubleType, true), new StructField("total_launches", DoubleType, true), new StructField("total_play_time", DoubleType, true),
            new StructField("avg_num_launches", DoubleType, true), new StructField("avg_time", DoubleType, true), new StructField("end_time", DoubleType, true),
            new StructField("last_played_on", DoubleType, true), new StructField("mean_play_time", DoubleType, true), new StructField("mean_play_time_interval", DoubleType, true),
            new StructField("num_contents", DoubleType, true), new StructField("num_days", DoubleType, true), new StructField("num_sessions", DoubleType, true),
            new StructField("play_start_time", DoubleType, true), new StructField("start_time", DoubleType, true))

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
        val testDataFile = config.getOrElse("testDataFile", "/tmp/test.dat.libfm").asInstanceOf[String]
        val outputFile = config.getOrElse("outputFile", "/tmp/score.txt").asInstanceOf[String]
        val libfmInputFile = config.getOrElse("libfmInputFile", "/tmp/libfm_input.csv").asInstanceOf[String]
        val model = config.getOrElse("model", "/tmp/fm.model").asInstanceOf[String]
        val libfmLogFile = config.getOrElse("libfmLogFile", "/tmp/logFile").asInstanceOf[String]
        val libfmExec = config.getOrElse("libfm.executable_path", "/usr/local/bin/") + "libFM";
        val bucket = config.getOrElse("bucket", "sandbox-data-store").asInstanceOf[String];
        val key = config.getOrElse("key", "model/fm.model").asInstanceOf[String];
        val libFMTrainConfig = config.getOrElse("libFMTrainConfig", "-dim 1,1,8 -iter 100 -method sgd -task r -regular 0,0,0.01 -learn_rate 0.1 -seed 100 -init_stdev 0.1")
        val libFMScoreConfig = config.getOrElse("libFMScoreConfig", "-dim 1,1,8 -iter 0 -method sgd -task r -regular 0,0,0.01 -learn_rate 0.1 -seed 100 -init_stdev 0.1")

        CommonUtil.deleteFile(libfmFile);
        CommonUtil.deleteFile(trainDataFile);
        CommonUtil.deleteFile(testDataFile);
        CommonUtil.deleteFile(outputFile);
        CommonUtil.deleteFile(model);
        CommonUtil.deleteFile(libfmInputFile);
        CommonUtil.deleteFile(libfmLogFile);

        JobLogger.log("Creating dataframe and libfm data", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        val rdd: RDD[Row] = _createDF(data);
        val df = sqlContext.createDataFrame(rdd, _getStructType);

        if (config.getOrElse("saveDataFrame", false).asInstanceOf[Boolean]) {
            val columnNames = df.columns.mkString(",")
            val rdd = df.map { x => x.mkString(",") }
            OutputDispatcher.dispatchDF(Dispatcher("file", Map("file" -> libfmInputFile)), rdd, columnNames);
        }
        val formula = new RFormula()
            .setFormula("c1_total_ts ~ .")
            .setFeaturesCol("features")
            .setLabelCol("label")
        val output = formula.fit(df).transform(df)
        val labeledRDD = output.select("features", "label").map { x => new LabeledPoint(x.getDouble(1), x.getAs[Vector](0)) };
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
        }.cache;

        JobLogger.log("Creating training dataset", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        val usedDataSet = dataStr.filter { x => !StringUtils.startsWith(x, "0.0") }
        var trainDataSet: RDD[String] = null
        var testDataSet: RDD[String] = null
        if (usedDataSet.count() < 100) {
            trainDataSet = usedDataSet
            testDataSet = usedDataSet
        } else {
            trainDataSet = usedDataSet.sample(false, 0.8, System.currentTimeMillis().toInt);
            testDataSet = usedDataSet.sample(false, 0.2, System.currentTimeMillis().toInt);
        }
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> trainDataFile)), trainDataSet);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> testDataFile)), testDataSet);
        JobLogger.log("Training dataset created", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus, "totalRecords" -> usedDataSet.count(), "numOfTrainRecords" -> trainDataSet.count(), "numOfTestRecords" -> testDataSet.count())), INFO);

        JobLogger.log("Training the model", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        ScriptDispatcher.dispatch(Array(), Map("script" -> s"$libfmExec -train $trainDataFile -test $testDataFile $libFMTrainConfig -rlog $libfmLogFile -save_model $model", "PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin")));

        val logLastLine = sc.textFile(libfmLogFile).collect().last
        val rmse = StringUtils.substring(logLastLine, 0, logLastLine.indexOf("\t"))
        JobLogger.log("The model is trained and reporting RMSE.", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus, "RMSE" -> rmse)), INFO);

        JobLogger.log("Creating scoring dataset", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> libfmFile)), dataStr);

        JobLogger.log("Running the scoring algorithm", Option(Map("memoryStatus" -> sc.getExecutorMemoryStatus)), INFO);
        ScriptDispatcher.dispatch(Array(), Map("script" -> s"$libfmExec -train $trainDataFile -test $libfmFile $libFMScoreConfig -out $outputFile -load_model $model", "PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin")));

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