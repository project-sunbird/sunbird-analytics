package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.model.cus_t
import org.ekstep.analytics.model.ContentFeatures
import org.ekstep.analytics.model.ContentFeatures_t

object ContentUsageTransformer extends DeviceRecommendationTransformer[ContentUsageSummaryFact, cus_t]{
  
    override def getTransformationByBinning(rdd: RDD[ContentUsageSummaryFact], num_bins: Int)(implicit sc: SparkContext): RDD[(String, cus_t)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.d_content_id, x.m_publish_date.getMillis.toDouble) };
        val f1_t = binning(f1, num_bins);
        val f2 = rdd.map { x => (x.d_content_id, x.m_last_sync_date.getMillis.toDouble) };
        val f2_t = binning(f2, num_bins);
        val f3 = rdd.map { x => (x.d_content_id, x.m_total_ts) };
        val f3_t = binning(f3, num_bins);
        val f4 = rdd.map { x => (x.d_content_id, x.m_total_sessions.toDouble) };
        val f4_t = binning(f4, num_bins);
        val f5 = rdd.map { x => (x.d_content_id, x.m_avg_ts_session) };
        val f5_t = binning(f5, num_bins);
        val f6 = rdd.map { x => (x.d_content_id, x.m_total_interactions.toDouble) };
        val f6_t = binning(f6, num_bins);
        val f7 = rdd.map { x => (x.d_content_id, x.m_avg_interactions_min) };
        val f7_t = binning(f7, num_bins);
        val f8 = rdd.map { x => (x.d_content_id, x.m_total_devices.toDouble) };
        val f8_t = binning(f8, num_bins);
        val f9 = rdd.map { x => (x.d_content_id, x.m_avg_sess_device) };
        val f9_t = binning(f9, num_bins);
        
        f1_t.join(f2_t).join(f3_t).join(f4_t).join(f5_t).join(f6_t).join(f7_t).join(f8_t).join(f9_t)
        .mapValues(f => cus_t(Option(f._1._1._1._1._1._1._1._1), Option(f._1._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._1._2), Option(f._1._1._1._1._1._2), Option(f._1._1._1._1._2), Option(f._1._1._1._2), Option(f._1._1._2), Option(f._1._2), Option(f._2)));
    }
    
    def getBinningForEOC(rdd: RDD[ContentFeatures], num_bins_downloads: Int, num_bins_rating: Int, num_bins_interactions: Int)(implicit sc: SparkContext): RDD[ContentFeatures_t] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.content_id, x.num_downloads.toDouble) };
        val f1_t = binning(f1, num_bins_downloads);
        val f2 = rdd.map { x => (x.content_id, x.avg_rating) };
        val f2_t = binning(f2, num_bins_rating);
        val f3 = rdd.map { x => (x.content_id, x.total_interactions.toDouble) };
        val f3_t = binning(f3, num_bins_interactions);
        
        f1_t.join(f2_t).join(f3_t)
        .map(f => ContentFeatures_t(f._1, f._2._1._1, f._2._1._2, f._2._2));
    }
}