package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.model.DeviceUsageSummary
import org.ekstep.analytics.model.dus_tf

object DeviceUsageTransformer extends DeviceRecommendationTransformer[DeviceUsageSummary, dus_tf] {
  
    override def getTransformationByBinning(rdd: RDD[DeviceUsageSummary])(implicit sc: SparkContext): RDD[(String, dus_tf)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.device_id, x.end_time.getOrElse(0L).toDouble) };
        val f1_t = binning(f1, 3);
        val f2 = rdd.map { x => (x.device_id, x.last_played_on.getOrElse(0L).toDouble) };
        val f2_t = binning(f2, 3);
        val f3 = rdd.map { x => (x.device_id, x.play_start_time.getOrElse(0L).toDouble) };
        val f3_t = binning(f3, 3);
        val f4 = rdd.map { x => (x.device_id, x.start_time.getOrElse(0L).toDouble) };
        val f4_t = binning(f4, 3);
        f1_t.join(f2_t).join(f3_t).join(f4_t).mapValues(f => dus_tf(Option(f._1._1._1), Option(f._1._1._2), Option(f._1._2), Option(f._2)));
    }
    
    override def removeOutliers(rdd: RDD[DeviceUsageSummary])(implicit sc: SparkContext): RDD[(String, DeviceUsageSummary)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        
        val inputRDD = rdd.map{ x => (x.device_id,x)}
        val ts = rdd.map { x => (x.device_id, x.total_timespent.getOrElse(0.0)) };
        val ts_t = outlierTreatment(ts);
        val total_launches = rdd.map { x => (x.device_id, x.total_launches.getOrElse(0L).toDouble) };
        val total_launches_t = outlierTreatment(total_launches);
        val total_play_time = rdd.map { x => (x.device_id, x.total_play_time.getOrElse(0.0)) };
        val total_play_time_t = outlierTreatment(total_play_time);
        val avg_num_launches = rdd.map { x => (x.device_id, x.avg_num_launches.getOrElse(0.0)) };
        val avg_num_launches_t = outlierTreatment(avg_num_launches);
        val avg_time = rdd.map { x => (x.device_id, x.avg_time.getOrElse(0.0)) };
        val avg_time_t = outlierTreatment(avg_time);
        val mean_play_time = rdd.map { x => (x.device_id, x.mean_play_time.getOrElse(0.0)) };
        val mean_play_time_t = outlierTreatment(mean_play_time);
        val mean_play_time_interval = rdd.map { x => (x.device_id, x.mean_play_time_interval.getOrElse(0.0)) };
        val mean_play_time_interval_t = outlierTreatment(mean_play_time_interval);
        val num_contents = rdd.map { x => (x.device_id, x.num_contents.getOrElse(0L).toDouble) };
        val num_contents_t = outlierTreatment(num_contents);
        val num_days = rdd.map { x => (x.device_id, x.num_days.getOrElse(0L).toDouble) };
        val num_days_t = outlierTreatment(num_days);
        val num_sessions = rdd.map { x => (x.device_id, x.num_sessions.getOrElse(0L).toDouble) };
        val num_sessions_t = outlierTreatment(num_sessions);
        
        val joinedData = inputRDD.join(ts_t).join(total_launches_t).join(total_play_time_t).join(avg_num_launches_t).join(avg_time_t).join(mean_play_time_t).join(mean_play_time_interval_t).join(num_contents_t).join(num_days_t).join(num_sessions_t)
        joinedData.map{ x =>
            val dus = x._2._1._1._1._1._1._1._1._1._1._1
            val ts_t = x._2._1._1._1._1._1._1._1._1._1._2
            val total_launches_t = x._2._1._1._1._1._1._1._1._1._2
            val total_play_time_t = x._2._1._1._1._1._1._1._1._2
            val avg_num_launches_t = x._2._1._1._1._1._1._1._2
            val avg_time_t = x._2._1._1._1._1._1._2
            val mean_play_time_t = x._2._1._1._1._1._2
            val mean_play_time_interval_t = x._2._1._1._1._2
            val num_contents_t = x._2._1._1._2
            val num_days_t = x._2._1._2
            val num_sessions_t = x._2._2
            (x._1, DeviceUsageSummary(dus.device_id, dus.start_time, dus.end_time, Option(num_days_t.toLong), Option(total_launches_t.toLong), Option(ts_t), Option(avg_num_launches_t), Option(avg_time_t), Option(num_contents_t.toLong), dus.play_start_time, dus.last_played_on, Option(total_play_time_t), Option(num_sessions_t.toLong), Option(mean_play_time_t), Option(mean_play_time_interval_t),dus.last_played_content))
        } 
    }
    
    override def excecute(rdd: RDD[DeviceUsageSummary])(implicit sc: SparkContext): RDD[(String,(DeviceUsageSummary, dus_tf))] = {
        
        val binning = getTransformationByBinning(rdd)
        val outlier = removeOutliers(rdd)
        outlier.join(binning);
    }
}