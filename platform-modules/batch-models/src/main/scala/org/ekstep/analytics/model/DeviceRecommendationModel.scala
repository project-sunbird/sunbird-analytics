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

case class DeviceMetrics(did: DeviceId, content_list: Map[String, ContentModel], device_usage: DeviceUsageSummary, device_spec: DeviceSpec, device_content: Map[String, DeviceContentSummary]);
case class DeviceContext(did: String, contentInFocus: String, contentInFocusModel: ContentModel, contentInFocusVec: ContentToVector, contentInFocusUsageSummary: DeviceContentSummary, otherContentId: String, otherContentModel: ContentModel, otherContentModelVec: ContentToVector, otherContentUsageSummary: DeviceContentSummary, device_usage: DeviceUsageSummary, device_spec: DeviceSpec) extends AlgoInput;
case class DeviceRecos(device_id: String, scores: List[(String, Option[Double])]) extends AlgoOutput with Output
case class ContentToVector(contentId: String, text_vec: Option[List[Double]], tag_vec: Option[List[Double]]);

object DeviceRecommendationModel extends IBatchModelTemplate[DerivedEvent, DeviceContext, DeviceRecos, DeviceRecos] with Serializable {

    val className = "org.ekstep.analytics.model.DeviceRecommendationEngine"
    override def name(): String = "DeviceRecommendationEngine"

    val defaultDSpec = DeviceSpec(null, "Other", "Other", "Android", "Other", -1, 0.0, 0.0, 0, "0,0", "Other", 0, null)
    val defaultDCUS = DeviceContentSummary(null, null, None, None, None, None, None, None, None, None, None, None, None, None)

    def choose[A](it: Buffer[A], r: Random): A = {
        val random_index = r.nextInt(it.size);
        it(random_index);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContext] = {

        val contentModel = ContentAdapter.getLiveContent().map { x => (x.id, x) }.toMap;
        val contentModelB = sc.broadcast(contentModel);
        val contentVectors = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC).map { x => (x.contentId, x) }.collect().toMap;
        val contentVectorsB = sc.broadcast(contentVectors);
        val device_usage = sc.cassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => (DeviceId(x.device_id), x) }
        val allDevices = device_usage.map(x => x._1).distinct; // TODO: Do we need distinct here???
        val device_spec = allDevices.joinWithCassandraTable[DeviceSpec](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE).map { x => (x._1, x._2) }

        val device_content_usage = allDevices.joinWithCassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).groupBy(f => f._1).mapValues(f => f.map(x => x._2));

        device_usage.leftOuterJoin(device_spec).leftOuterJoin(device_content_usage).map { x =>
            val dc = x._2._2.getOrElse(Buffer[DeviceContentSummary]()).map { x => (x.content_id, x) }.toMap;
            DeviceMetrics(x._1, contentModelB.value, x._2._1._1, x._2._1._2.getOrElse(defaultDSpec), dc)
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
                    DeviceContext(x.did.device_id, y._1, y._2, contentInFocusVec, contentInFocusUsageSummary, otherContentId, otherContentModel, otherContentModelVec, randomContent, x.device_usage, x.device_spec);
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
            if(psc.length == 0) {
                seq ++= Seq(0.0, 0.0);    
            } else if(psc.length == 1) {
                seq ++= Seq(if(StringUtils.isBlank(psc(0))) 0.0 else psc(0).toDouble, 0.0);
            } else {
                seq ++= Seq(if(StringUtils.isBlank(psc(0))) 0.0 else psc(0).toDouble, if(StringUtils.isBlank(psc(1))) 0.0 else psc(1).toDouble);
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

        val rdd: RDD[Row] = _createDF(data);
        val sqlContext = new SQLContext(sc);
        val df = sqlContext.createDataFrame(rdd, _getStructType);
        //df.printSchema();
        if(config.getOrElse("saveDataFrame", false).asInstanceOf[Boolean])
            OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "libfm.input")), df.toJSON);
        
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
        }.cache();
        
        val libfmFile = config.getOrElse("dataFile", "score.dat.libfm").asInstanceOf[String]
        val trainDataFile = config.getOrElse("trainDataFile", "train.dat.libfm").asInstanceOf[String]
        val testDataFile = config.getOrElse("testDataFile", "test.dat.libfm").asInstanceOf[String]
        val outputFile = config.getOrElse("outputFile", "score.txt").asInstanceOf[String]
        val model = config.getOrElse("model", "fm.model").asInstanceOf[String]
        
        CommonUtil.deleteFile(libfmFile);
        CommonUtil.deleteFile(trainDataFile);
        CommonUtil.deleteFile(testDataFile);
        
        val usedDataSet = dataStr.filter { x => (!("0.0".equals(x.substring(0, x.indexOf(' '))))) }
        val trainDataSet = usedDataSet.sample(false, 0.8, System.currentTimeMillis().toInt)
        val testDataSet = usedDataSet.sample(false, 0.2, System.currentTimeMillis().toInt)
        
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> trainDataFile)), trainDataSet);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> testDataFile)), testDataSet);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> libfmFile)), dataStr);
        
        // CommonUtil.deleteDirectory("libsvm/");
        // MLUtils.saveAsLibSVMFile(labeledRDD, "libsvm/");
        val libfmExec = config.getOrElse("libfm.executable_path", "/usr/local/bin/") + "libFM";
        val dim = config.getOrElse("dim", "1,1,10")
        val iter = config.getOrElse("iter", 10)
        val method = config.getOrElse("method", "sgd")
        val task = config.getOrElse("task", "r")
        val regular = config.getOrElse("regular", "1,1,1")
        val learn_rate = config.getOrElse("learn_rate", 0.1)
        val seed = config.getOrElse("seed", 100)
        // 1. Invoke training
        ScriptDispatcher.dispatch(Array(), Map("script" -> s"$libfmExec -train $trainDataFile -test $testDataFile -dim $dim -iter $iter -method $method -task $task -regular $regular -learn_rate $learn_rate -seed $seed -save_model $model",
                "PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin")))
        
        // 2. Invoke scoring
        ScriptDispatcher.dispatch(Array(), Map("script" -> s"$libfmExec -train $trainDataFile -test $libfmFile -dim $dim -iter $iter -method $method -task $task -regular $regular -learn_rate $learn_rate -seed $seed -out $outputFile -load_model $model",
                "PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin")))                
        // 3. Save model to S3
        //S3Dispatcher.dispatch(null, Map("filepath" -> "fm.model", "bucket" -> "sandbox-data-store", "key" -> "model/fm.model"))
                
        // 4. Load libsvm output file and transform to DeviceRecos
        // TODO: Read output file score.out and save the recommendations into device recos
        val device_content = data.map{x => (x.did, x.contentInFocus)}.zipWithIndex()
        val dcWithIndexKey = device_content.map{case (k,v) => (v,k)}
        val scores = sc.textFile("score.txt").map{x => x.toDouble}.zipWithIndex()
        val scoresWithIndexKey = scores.map{case (k,v) => (v,k)}
        val device_scores = dcWithIndexKey.leftOuterJoin(scoresWithIndexKey).map{x => x._2}.groupBy(x => x._1._1).mapValues(f => f.map(x => (x._1._2, x._2)).toList.sortBy(y => y._2))
        device_scores.map{ x =>
                DeviceRecos(x._1, x._2)
        }
    }

    override def postProcess(data: RDD[DeviceRecos], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceRecos] = {

        data.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_RECOS)
        data;
    }

}