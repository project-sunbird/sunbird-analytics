package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.model.DeviceContentSummary
import org.ekstep.analytics.model.dcus_tf

object DeviceContentUsageTransformer extends RETransformer[DeviceContentSummary, dcus_tf] {
 
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
    
    override def removeOutliers(rdd: RDD[DeviceContentSummary])(implicit sc: SparkContext): RDD[DeviceContentSummary] = {
        
        null
    }
}