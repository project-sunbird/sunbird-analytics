package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.model.DeviceUsageSummary
import org.ekstep.analytics.model.dus_tf

object DeviceUsageTransformer extends RETransformer[DeviceUsageSummary, dus_tf] {
  
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
    
    override def removeOutliers(rdd: RDD[DeviceUsageSummary])(implicit sc: SparkContext): RDD[DeviceUsageSummary] = {
        
        null
    }
}