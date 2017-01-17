package org.ekstep.analytics.model

import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.joda.time.DateTime
import org.ekstep.analytics.transformer.DeviceUsageTransformer
import org.ekstep.analytics.transformer.ContentUsageTransformer
import org.ekstep.analytics.transformer.DeviceContentUsageTransformer
import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.ekstep.analytics.transformer.DeviceSpecTransformer
import org.ekstep.analytics.adapter.ContentModel
import org.ekstep.analytics.updater.DeviceSpec
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.{DenseVector => BDV}

case class DevSpec(device_id: String, make: String, internal_disk: Double, external_disk: Double, screen_size: Double, primary_camera: Double, secondary_camera: Double)
case class RawInput(did: String, c1_content_id: String, c2_content_id: String, c3_content_id: String, c1_text: Seq[Double], c1_tag: Seq[Double],
                    c1_total_ts: Double, c1_subject: String, c1_contentType: String, c1_language: String, c2_text: Seq[Double],
                    c2_tag: Seq[Double], c2_subject: String, c2_contentType: String, c2_language: String, c3_text: Seq[Double], c3_tag: Seq[Double])

object RETestModel extends App {

    val sc = CommonUtil.getSparkContext(2, "test")
    implicit val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._

    val inputRDD = sc.textFile("src/test/resources/device-recos-training/RE-data/training/2017-01-04/01-20/RE-input")
    val input = inputRDD.map { f => JSONUtils.deserialize[Map[String, AnyRef]](f) }

    val rawInput = input.map { f =>
        (f.get("did").get.asInstanceOf[String], RawInput(f.get("did").get.asInstanceOf[String], f.get("c1_content_id").get.asInstanceOf[String], f.get("c2_content_id").get.asInstanceOf[String], f.get("c3_content_id").get.asInstanceOf[String], f.get("c1_text").get.asInstanceOf[Seq[Double]], f.get("c1_tag").get.asInstanceOf[Seq[Double]], f.get("c1_total_ts").get.asInstanceOf[Double], f.get("c1_subject").get.asInstanceOf[String], f.get("c1_contentType").get.asInstanceOf[String], f.get("c1_language").get.asInstanceOf[String], f.get("c2_text").get.asInstanceOf[Seq[Double]], f.get("c2_tag").get.asInstanceOf[Seq[Double]], f.get("c2_subject").get.asInstanceOf[String], f.get("c2_contentType").get.asInstanceOf[String], f.get("c2_language").get.asInstanceOf[String], f.get("c3_text").get.asInstanceOf[Seq[Double]], f.get("c3_tag").get.asInstanceOf[Seq[Double]]))
    }
    println(rawInput.first()._2)
    // c1 cus binning
    val c1CUS = input.map { f =>
        ContentUsageSummaryFact(0, f.get("c1_content_id").get.asInstanceOf[String], "all", new DateTime(f.get("c1_publish_date").get.toString().toLong), new DateTime(f.get("c1_last_sync_date").get.toString().toLong), new DateTime(0), f.get("c1_total_timespent").get.asInstanceOf[Double], f.get("c1_total_sessions").get.toString().toLong, f.get("c1_avg_ts_session").get.asInstanceOf[Double], f.get("c1_num_interactions").get.toString().toLong, f.get("c1_mean_interactions_min").get.asInstanceOf[Double], f.get("c1_total_devices").get.toString().toLong, f.get("c1_avg_sess_devices").get.asInstanceOf[Double], null);
    }
    val c1CUST = ContentUsageTransformer.getTransformationByBinning(c1CUS, 4)(sc).map { x => (x._1, x._2) };
    //val c1CUSTB = sc.broadcast(c1CUST)
    //c1CUS.unpersist(true);
    println("c1 cus binning done")
    // c2 cus binning
    val c2CUS = input.map { f =>
        ContentUsageSummaryFact(0, f.get("c2_content_id").get.asInstanceOf[String], "all", new DateTime(f.get("c2_publish_date").get.toString().toLong), new DateTime(f.get("c2_last_sync_date").get.toString().toLong), new DateTime(0), f.get("c2_total_timespent").get.asInstanceOf[Double], f.get("c2_total_sessions").get.toString().toLong, f.get("c2_avg_ts_session").get.asInstanceOf[Double], f.get("c2_num_interactions").get.toString().toLong, f.get("c2_mean_interactions_min").get.asInstanceOf[Double], f.get("c2_total_devices").get.toString().toLong, f.get("c2_avg_sess_devices").get.asInstanceOf[Double], null);
    }
    val c2CUST = ContentUsageTransformer.getTransformationByBinning(c2CUS, 4)(sc).map { x => (x._1, x._2) } //.collect().toMap;
    //val c2CUSTB = sc.broadcast(c2CUST)
    //c2CUS.unpersist(true);
    println("c2 cus binning done")
    // c2 dcus binning
    val c2DCUS = input.map { f =>
        val downloaded = if (f.get("c2_downloaded").get.asInstanceOf[Int] == 1) true else false
        DeviceContentSummary(f.get("did").get.asInstanceOf[String], f.get("c2_content_id").get.asInstanceOf[String], None, Option(f.get("c2_num_sessions").get.toString().toLong), Option(f.get("c2_total_interactions").get.toString().toLong), Option(f.get("c2_avg_interactions_min").get.asInstanceOf[Double]), Option(f.get("c2_total_ts").get.asInstanceOf[Double]), Option(f.get("c2_last_played_on").get.toString().toLong), Option(f.get("c2_start_time").get.toString().toLong), Option(f.get("c2_mean_play_time_interval").get.asInstanceOf[Double]), Option(downloaded), Option(f.get("c2_download_date").get.toString().toLong), Option(f.get("c2_num_group_user").get.toString().toLong), Option(f.get("c2_num_individual_user").get.toString().toLong))
    }
    val c2DCUST = DeviceContentUsageTransformer.getTransformationByBinning(c2DCUS, 4)(sc).map { x => (x._1, x._2) } //.collect().toMap;
    //val c2DCUSTB = sc.broadcast(c2DCUST)
    //c2DCUS.unpersist(true);
    println("c2 dcus binning done")
    // dus binning
    val dus = input.map { f =>
        DeviceUsageSummary(f.get("did").get.asInstanceOf[String], Option(f.get("start_time").get.toString().toLong), Option(f.get("end_time").get.toString().toLong), Option(f.get("num_days").get.toString().toLong), Option(f.get("total_launches").get.toString().toLong), Option(f.get("total_timespent").get.asInstanceOf[Double]), Option(f.get("avg_num_launches").get.asInstanceOf[Double]), Option(f.get("avg_time").get.asInstanceOf[Double]), Option(f.get("num_contents").get.toString().toLong), Option(f.get("play_start_time").get.toString().toLong), Option(f.get("last_played_on").get.toString().toLong), Option(f.get("total_play_time").get.asInstanceOf[Double]), Option(f.get("num_sessions").get.toString().toLong), Option(f.get("mean_play_time").get.asInstanceOf[Double]), Option(f.get("mean_play_time_interval").get.asInstanceOf[Double]), Option(f.get("c3_content_id").get.asInstanceOf[String]))
    }
    val dusT = DeviceUsageTransformer.getTransformationByBinning(dus, 4)(sc)
    println("dus binning done")
    // deviceSpec binning
    val deviceSpec = input.map { f =>
        DevSpec(f.get("did").get.asInstanceOf[String], f.get("device_spec").get.asInstanceOf[String], f.get("internal_disk").get.asInstanceOf[Double], f.get("external_disk").get.asInstanceOf[Double], f.get("screen_size").get.asInstanceOf[Double], f.get("primary_camera").get.asInstanceOf[Double], f.get("secondary_camera").get.asInstanceOf[Double])
    }
//    val deviceSpecT = DeviceSpecTransformer.getTransformationByBinning(deviceSpec, 4)(sc)
//    println("deviceSpec binning done")
//    println(deviceSpecT.first()._2)

    val defaultDCUS = DeviceContentSummary(null, null, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultDUS = DeviceUsageSummary(null, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    val defaultCUS = ContentUsageSummaryFact(0, null, null, new DateTime(0), new DateTime(0), new DateTime(0), 0.0, 0L, 0.0, 0L, 0.0, 0, 0.0, null);
//    val out = rawInput.leftOuterJoin(dusT).leftOuterJoin(deviceSpec).map { x =>
//
//        val contentInFocusModel = ContentModel(x._2._1._1.c1_content_id, List(x._2._1._1.c1_subject), x._2._1._1.c1_contentType, List(x._2._1._1.c1_language))
//        val contentInFocusVec = ContentToVector(x._2._1._1.c1_content_id, Option(x._2._1._1.c1_text.toList), Option(x._2._1._1.c1_tag.toList))
//        val contentInFocusUsageSummary = DeviceContentSummary(x._1, x._2._1._1.c1_content_id, None, None, None, None, Option(x._2._1._1.c1_total_ts), None, None, None, None, None, None, None)
//        val otherContentModel = ContentModel(x._2._1._1.c2_content_id, List(x._2._1._1.c2_subject), x._2._1._1.c2_contentType, List(x._2._1._1.c2_language))
//        val otherContentModelVec = ContentToVector(x._2._1._1.c2_content_id, Option(x._2._1._1.c2_text.toList), Option(x._2._1._1.c2_tag.toList))
//        val deviceSpec = DeviceSpec(x._1, "", "", "", x._2._2.get.make, 0.0, x._2._2.get.internal_disk, x._2._2.get.external_disk, x._2._2.get.screen_size, (x._2._2.get.primary_camera + "," + x._2._2.get.secondary_camera), "", 0.0, List())
//        val lastPlayedContentVec = ContentToVector(x._2._1._1.c3_content_id, Option(x._2._1._1.c3_text.toList), Option(x._2._1._1.c3_tag.toList))
//        DeviceContext(x._1, x._2._1._1.c1_content_id, contentInFocusModel, contentInFocusVec, contentInFocusUsageSummary, c1CUST.lookup(x._2._1._1.c1_content_id)(0), x._2._1._1.c2_content_id, otherContentModel, otherContentModelVec, defaultDCUS, defaultCUS, defaultDUS, deviceSpec, c2CUST.lookup(x._2._1._1.c2_content_id)(0), x._2._1._2.get, c2DCUST.lookup(x._2._1._1.c2_content_id)(0), lastPlayedContentVec);
//    }
//    println(out.first())
}