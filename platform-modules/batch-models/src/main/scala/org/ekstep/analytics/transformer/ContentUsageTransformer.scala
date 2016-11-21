package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.model.cus_t

object ContentUsageTransformer extends DeviceRecommendationTransformer[ContentUsageSummaryFact, cus_t]{
  
    override def getTransformationByBinning(rdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): RDD[(String, cus_t)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.d_content_id, x.m_publish_date.getMillis.toDouble) };
        val f1_t = binning(f1, 3);
        val f2 = rdd.map { x => (x.d_content_id, x.m_last_sync_date.getMillis.toDouble) };
        val f2_t = binning(f2, 3);
        val f3 = rdd.map { x => (x.d_content_id, x.m_total_ts) };
        val f3_t = binning(f3, 3);
        val f4 = rdd.map { x => (x.d_content_id, x.m_total_sessions.toDouble) };
        val f4_t = binning(f4, 3);
        val f5 = rdd.map { x => (x.d_content_id, x.m_avg_ts_session) };
        val f5_t = binning(f5, 3);
        val f6 = rdd.map { x => (x.d_content_id, x.m_total_interactions.toDouble) };
        val f6_t = binning(f6, 3);
        val f7 = rdd.map { x => (x.d_content_id, x.m_avg_interactions_min) };
        val f7_t = binning(f7, 3);
        val f8 = rdd.map { x => (x.d_content_id, x.m_total_devices.toDouble) };
        val f8_t = binning(f8, 3);
        val f9 = rdd.map { x => (x.d_content_id, x.m_avg_sess_device) };
        val f9_t = binning(f9, 3);
        
        f1_t.join(f2_t).join(f3_t).join(f4_t).join(f5_t).join(f6_t).join(f7_t).join(f8_t).join(f9_t)
        .mapValues(f => cus_t(Option(f._1._1._1._1._1._1._1._1), Option(f._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._2), Option(f._1._1._1._1._2), Option(f._1._1._1._2), Option(f._1._1._2), Option(f._1._2), Option(f._2)));
    }
}