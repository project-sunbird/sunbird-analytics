package org.ekstep.analytics.transformer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.ContentUsageSummaryFact
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._

class TestContentUsageTransformer extends SparkSpec(null) {

    "ContentUsageTransformer" should "perform binning and outlier on CUS" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600, 1459641600, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_68601', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600, 1459641600, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63848', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600, 1459641600, 4, 0, 0, 20);");

        }

        val table = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)
        val out = ContentUsageTransformer.excecute(table)
        out.count() should be(table.count())
    }

}