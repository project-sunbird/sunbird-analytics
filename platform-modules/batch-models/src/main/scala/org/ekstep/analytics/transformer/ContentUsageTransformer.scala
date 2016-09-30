package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.updater.ContentUsageSummaryFact
import org.ekstep.analytics.model.cus_t

object ContentUsageTransformer extends RETransformer[ContentUsageSummaryFact, cus_t]{
  
    override def getTransformationByBinning(rdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): RDD[(String, cus_t)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        val f1 = rdd.map { x => (x.d_content_id, x.m_publish_date.getMillis.toDouble) };
        val f1_t = binning(f1, 3);
        val f2 = rdd.map { x => (x.d_content_id, x.m_last_sync_date.getMillis.toDouble) };
        val f2_t = binning(f2, 3);
        f1_t.join(f2_t).mapValues(f => cus_t(Option(f._1), Option(f._2)));
    }
    
    override def removeOutliers(rdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): RDD[(String, ContentUsageSummaryFact)] = {
        
        implicit val sqlContext = new SQLContext(sc);
        
        val inputRDD = rdd.map{ x => (x.d_content_id,x)}
        val ts = rdd.map { x => (x.d_content_id, x.m_total_ts) };
        val ts_t = outlierTreatment(ts);
        val total_sessions = rdd.map { x => (x.d_content_id, x.m_total_sessions.toDouble) };
        val total_sessions_t = outlierTreatment(total_sessions);
        val avg_ts_session = rdd.map { x => (x.d_content_id, x.m_avg_ts_session) };
        val avg_ts_session_t = outlierTreatment(avg_ts_session);
        val num_interactions = rdd.map { x => (x.d_content_id, x.m_total_interactions.toDouble) };
        val num_interactions_t = outlierTreatment(num_interactions);
        val mean_interactions_min = rdd.map { x => (x.d_content_id, x.m_avg_interactions_min) };
        val mean_interactions_min_t = outlierTreatment(mean_interactions_min);
        
        val joinedData = inputRDD.join(ts_t).join(total_sessions_t).join(avg_ts_session_t).join(num_interactions_t).join(mean_interactions_min_t)
        joinedData.map{ x =>
            val cus = x._2._1._1._1._1._1
            val ts_t = x._2._1._1._1._1._2
            val total_sessions_t = x._2._1._1._1._2
            val avg_ts_session_t = x._2._1._1._2
            val num_interactions_t = x._2._1._2
            val mean_interactions_min_t = x._2._2
            (x._1, ContentUsageSummaryFact(cus.d_period, x._1, cus.d_tag, cus.m_publish_date, cus.m_last_sync_date, cus.m_last_gen_date, ts_t, total_sessions_t.toLong, avg_ts_session_t, num_interactions_t.toLong, mean_interactions_min_t, cus.m_total_devices, cus.m_avg_sess_device, cus.m_device_ids))
        } 
    }
    
    def excecute(rdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): RDD[(String,(ContentUsageSummaryFact, cus_t))] = {
        
        val binning = getTransformationByBinning(rdd)
        val outlier = removeOutliers(rdd)
        outlier.join(binning);
    }
}