package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.model.DeviceContentSummary
import org.ekstep.analytics.model.dcus_tf

object DeviceContentUsageTransformer extends DeviceRecommendationTransformer[DeviceContentSummary, dcus_tf] {
 
    override def getTransformationByBinning(rdd: RDD[DeviceContentSummary])(implicit sc: SparkContext): RDD[(String, dcus_tf)] = {
        
        val indexedRDD = rdd.zipWithIndex().map { case (k, v) => (v, k) }.map(x => (x._1.toString(), x._2))
        implicit val sqlContext = new SQLContext(sc);
        val f1 = indexedRDD.map { x => (x._1, x._2.download_date.getOrElse(0L).toDouble) };
        val f1_t = binning(f1, 3);
        val f2 = indexedRDD.map { x => (x._1, x._2.last_played_on.getOrElse(0L).toDouble) };
        val f2_t = binning(f2, 3);
        val f3 = indexedRDD.map { x => (x._1, x._2.start_time.getOrElse(0L).toDouble) };
        val f3_t = binning(f3, 3);
        val x = f1_t.join(f2_t).join(f3_t) //.mapValues(f => dcus_tf(f._1._1, f._1._2, f._2));
        indexedRDD.leftOuterJoin(x).map(f => (f._2._1.device_id, dcus_tf(f._2._1.content_id, Option(f._2._2.get._1._1), Option(f._2._2.get._1._2), Option(f._2._2.get._2))));
    }
    
    override def removeOutliers(rdd: RDD[DeviceContentSummary])(implicit sc: SparkContext): RDD[(String, DeviceContentSummary)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        
        val inputRDD = rdd.zipWithIndex().map { case (k, v) => (v, k) }.map(x => (x._1.toString(), x._2))
        val ts = inputRDD.map { x => (x._1, x._2.total_timespent.getOrElse(0.0)) };
        val ts_t = outlierTreatment(ts);
        val avg_interactions_min = inputRDD.map { x => (x._1, x._2.avg_interactions_min.getOrElse(0.0)) };
        val avg_interactions_min_t = outlierTreatment(avg_interactions_min);
        val mean_play_time_interval = inputRDD.map { x => (x._1, x._2.mean_play_time_interval.getOrElse(0.0)) };
        val mean_play_time_interval_t = outlierTreatment(mean_play_time_interval);
        val num_group_user = inputRDD.map { x => (x._1, x._2.num_group_user.getOrElse(0L).toDouble) };
        val num_group_user_t = outlierTreatment(num_group_user);
        val num_individual_user = inputRDD.map { x => (x._1, x._2.num_individual_user.getOrElse(0L).toDouble) };
        val num_individual_user_t = outlierTreatment(num_individual_user);
        val num_sessions = inputRDD.map { x => (x._1, x._2.num_sessions.getOrElse(0L).toDouble) };
        val num_sessions_t = outlierTreatment(num_sessions);
        val total_interactions = inputRDD.map { x => (x._1, x._2.total_interactions.getOrElse(0L).toDouble) };
        val total_interactions_t = outlierTreatment(total_interactions);
        
        val joinedData = inputRDD.join(ts_t).join(avg_interactions_min_t).join(mean_play_time_interval_t).join(num_group_user_t).join(num_individual_user_t).join(num_sessions_t).join(total_interactions_t)
        joinedData.map{ x =>
            val dcus = x._2._1._1._1._1._1._1._1
            val ts_t = x._2._1._1._1._1._1._1._2
            val avg_interactions_min_t = x._2._1._1._1._1._1._2
            val mean_play_time_interval_t = x._2._1._1._1._1._2
            val num_group_user_t = x._2._1._1._1._2
            val num_individual_user_t = x._2._1._1._2
            val num_sessions_t = x._2._1._2
            val total_interactions_t = x._2._2
            (x._1, DeviceContentSummary(dcus.device_id, dcus.content_id, dcus.game_ver, Option(num_sessions_t.toLong), Option(total_interactions_t.toLong), Option(avg_interactions_min_t), Option(ts_t), dcus.last_played_on, dcus.start_time, Option(mean_play_time_interval_t), dcus.downloaded, dcus.download_date, Option(num_group_user_t.toLong), Option(num_individual_user_t.toLong)))
        } 
    }
    
    override def excecute(rdd: RDD[DeviceContentSummary])(implicit sc: SparkContext): RDD[(String,(DeviceContentSummary, dcus_tf))] = {
        
        val binning = getTransformationByBinning(rdd)
        val outlier = removeOutliers(rdd)
        outlier.join(binning);
    }
}