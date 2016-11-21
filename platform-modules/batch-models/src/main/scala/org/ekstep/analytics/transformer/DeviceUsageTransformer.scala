package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.model.DeviceUsageSummary
import org.ekstep.analytics.model.dus_tf

object DeviceUsageTransformer extends DeviceRecommendationTransformer[DeviceUsageSummary, dus_tf] {
  
    override def getTransformationByBinning(rdd: RDD[DeviceUsageSummary])(implicit sc: SparkContext): RDD[(String, dus_tf)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.device_id, x.start_time.getOrElse(0L).toDouble) };
        val f1_t = binning(f1, 3);
        val f2 = rdd.map { x => (x.device_id, x.end_time.getOrElse(0L).toDouble) };
        val f2_t = binning(f2, 3);
        val f3 = rdd.map { x => (x.device_id, x.num_days.getOrElse(0L).toDouble) };
        val f3_t = binning(f3, 3);
        val f4 = rdd.map { x => (x.device_id, x.total_launches.getOrElse(0L).toDouble) };
        val f4_t = binning(f4, 3);
        val f5 = rdd.map { x => (x.device_id, x.total_timespent.getOrElse(0.0)) };
        val f5_t = binning(f5, 3);
        val f6 = rdd.map { x => (x.device_id, x.avg_num_launches.getOrElse(0.0)) };
        val f6_t = binning(f6, 3);
        val f7 = rdd.map { x => (x.device_id, x.avg_time.getOrElse(0.0)) };
        val f7_t = binning(f7, 3);
        val f8 = rdd.map { x => (x.device_id, x.num_contents.getOrElse(0L).toDouble) };
        val f8_t = binning(f8, 3);
        val f9 = rdd.map { x => (x.device_id, x.play_start_time.getOrElse(0L).toDouble) };
        val f9_t = binning(f9, 3);
        val f10 = rdd.map { x => (x.device_id, x.last_played_on.getOrElse(0L).toDouble) };
        val f10_t = binning(f10, 3);
        val f11 = rdd.map { x => (x.device_id, x.total_play_time.getOrElse(0.0)) };
        val f11_t = binning(f11, 3);
        val f12 = rdd.map { x => (x.device_id, x.num_sessions.getOrElse(0L).toDouble) };
        val f12_t = binning(f12, 3);
        val f13 = rdd.map { x => (x.device_id, x.mean_play_time.getOrElse(0.0)) };
        val f13_t = binning(f13, 3);
        val f14 = rdd.map { x => (x.device_id, x.mean_play_time_interval.getOrElse(0.0)) };
        val f14_t = binning(f14, 3);
        
        f1_t.join(f2_t).join(f3_t).join(f4_t).join(f5_t).join(f6_t).join(f7_t).join(f8_t).join(f9_t).join(f10_t).join(f11_t).join(f12_t).join(f13_t).join(f14_t)
        .mapValues(f => dus_tf(Option(f._1._1._1._1._1._1._1._1._1._1._1._1._1), Option(f._1._1._1._1._1._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._2), Option(f._1._1._1._1._2), Option(f._1._1._1._2), Option(f._1._1._2), Option(f._1._2), Option(f._2)));
    }
}