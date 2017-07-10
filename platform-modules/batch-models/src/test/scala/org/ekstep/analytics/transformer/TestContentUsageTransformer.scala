package org.ekstep.analytics.transformer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.ContentUsageSummaryFact
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.model.ContentFeatures

class TestContentUsageTransformer extends SparkSpec(null) {

    "ContentUsageTransformer" should "perform binning and outlier on CUS" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, d_app_id, d_channel, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63844', 'Genie', 'Ekstep', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600, 1459641600, 4, 0, 0, -200);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, d_app_id, d_channel, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_68601', 'Genie', 'Ekstep', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600, 1459641600, 4, 0, 10, 1);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, d_app_id, d_channel, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63848', 'Genie', 'Ekstep', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600, 1459641600, 4, 0, 0, 2);");

        }

        val table = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)
        val out = ContentUsageTransformer.getTransformationByBinning(table, 4)
        out.count() should be(table.count())
    }
    
    it should "perform binning on ContentFeatures" in {

        val in = List(ContentFeatures("domain_63844", 5, 10.0, 10), ContentFeatures("domain_83844", 50, 100.0, 20), ContentFeatures("domain_43844", 25, 40.0, 4), ContentFeatures("domain_23844", 500, 120.0, 150))
        val input = sc.parallelize(in)
        val out = ContentUsageTransformer.getBinningForEOC(input, 10, 10, 10)
        out.count() should be(input.count())
    }

}