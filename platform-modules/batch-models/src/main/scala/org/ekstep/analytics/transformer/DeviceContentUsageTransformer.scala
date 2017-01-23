package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.model.DeviceContentSummary
import org.ekstep.analytics.model.dcus_tf

object DeviceContentUsageTransformer extends DeviceRecommendationTransformer[DeviceContentSummary, dcus_tf] {
 
    override def getTransformationByBinning(rdd: RDD[DeviceContentSummary], num_bins: Int)(implicit sc: SparkContext): RDD[(String, dcus_tf)] = {
        
        val indexedRDD = rdd.zipWithIndex().map { case (k, v) => (v, k) }.map(x => (x._1.toString(), x._2))
        implicit val sqlContext = new SQLContext(sc);
        val f1 = indexedRDD.map { x => (x._1, x._2.num_sessions.getOrElse(0L).toDouble) };
        val f1_t = binning(f1, num_bins);
        val f2 = indexedRDD.map { x => (x._1, x._2.total_interactions.getOrElse(0L).toDouble) };
        val f2_t = binning(f2, num_bins);
        val f3 = indexedRDD.map { x => (x._1, x._2.avg_interactions_min.getOrElse(0.0)) };
        val f3_t = binning(f3, num_bins);
        val f4 = indexedRDD.map { x => (x._1, x._2.total_timespent.getOrElse(0.0)) };
        val f4_t = binning(f4, num_bins);
        val f5 = indexedRDD.map { x => (x._1, x._2.last_played_on.getOrElse(0L).toDouble) };
        val f5_t = binning(f5, num_bins);
        val f6 = indexedRDD.map { x => (x._1, x._2.start_time.getOrElse(0L).toDouble) };
        val f6_t = binning(f6, num_bins);
        val f7 = indexedRDD.map { x => (x._1, x._2.mean_play_time_interval.getOrElse(0.0)) };
        val f7_t = binning(f7, num_bins);
        val f8 = indexedRDD.map { x => (x._1, x._2.download_date.getOrElse(0L).toDouble) };
        val f8_t = binning(f8, num_bins);
        val f9 = indexedRDD.map { x => (x._1, x._2.num_group_user.getOrElse(0L).toDouble) };
        val f9_t = binning(f9, num_bins);
        val f10 = indexedRDD.map { x => (x._1, x._2.num_individual_user.getOrElse(0L).toDouble) };
        val f10_t = binning(f10, num_bins);
        val x = f1_t.join(f2_t).join(f3_t).join(f4_t).join(f5_t).join(f6_t).join(f7_t).join(f8_t).join(f9_t).join(f10_t)
        indexedRDD.leftOuterJoin(x).map(f => (f._1, dcus_tf(f._2._1.content_id, Option(f._2._2.get._1._1._1._1._1._1._1._1._1), Option(f._2._2.get._1._1._1._1._1._1._1._1._2), Option(f._2._2.get._1._1._1._1._1._1._1._2), Option(f._2._2.get._1._1._1._1._1._1._2), Option(f._2._2.get._1._1._1._1._1._2), Option(f._2._2.get._1._1._1._1._2), Option(f._2._2.get._1._1._1._2), Option(f._2._2.get._1._1._2), Option(f._2._2.get._1._2), Option(f._2._2.get._2))));
    }
}