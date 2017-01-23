package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.updater.DeviceSpec
import org.apache.commons.lang3.StringUtils

object DeviceSpecTransformer extends DeviceRecommendationTransformer[DeviceSpec, DeviceSpec] {
  
    override def getTransformationByBinning(rdd: RDD[DeviceSpec],  num_bins: Int)(implicit sc: SparkContext): RDD[(String, DeviceSpec)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val input = rdd.map { x => (x.device_id, x) };
        val f1 = rdd.map { x => (x.device_id, x.internal_disk) };
        val f1_t = binning(f1, num_bins);
        val f2 = rdd.map { x => (x.device_id, x.external_disk) };
        val f2_t = binning(f2, num_bins);
        val f3 = rdd.map { x => (x.device_id, x.screen_size) };
        val f3_t = binning(f3, num_bins);
        val psc = input.mapValues{x => 
            val cam = x.primary_secondary_camera.split(",");
            if (cam.length == 0) {
                (0.0, 0.0);
            } else if (cam.length == 1) {
                (if (StringUtils.isBlank(cam(0))) 0.0 else cam(0).toDouble, 0.0);
            } else {
                (if (StringUtils.isBlank(cam(0))) 0.0 else cam(0).toDouble, if (StringUtils.isBlank(cam(1))) 0.0 else cam(1).toDouble);
            }
        }
        val f4 = psc.map { x => (x._1, x._2._1) };
        val f4_t = binning(f4, num_bins);
        val f5 = psc.map { x => (x._1, x._2._2) };
        val f5_t = binning(f5, num_bins);
        val psc_t = f4_t.join(f5_t).mapValues{f => List(f._1,f._2).mkString(",")}
        
        input.join(f1_t).join(f2_t).join(f3_t).join(psc_t)
        .mapValues(f => DeviceSpec(f._1._1._1._1.device_id, f._1._1._1._1.device_name, f._1._1._1._1.device_local_name, f._1._1._1._1.os, f._1._1._1._1.make, f._1._1._1._1.memory, f._1._1._1._2, f._1._1._2, f._1._2, f._2, f._1._1._1._1.cpu, f._1._1._1._1.num_sims, f._1._1._1._1.capabilities));
    }
}