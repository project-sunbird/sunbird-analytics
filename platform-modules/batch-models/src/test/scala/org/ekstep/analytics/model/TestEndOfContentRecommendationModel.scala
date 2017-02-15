package org.ekstep.analytics.model

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants

class TestEndOfContentRecommendationModel extends SparkSpec(null) {
  
    "EndOfContentRecommendationModel" should "save scores to cassandra with filtering blacklisted contents" in {
        
        populateDatabase();
        val me = EndOfContentRecommendationModel.execute(null, Option(Map("method" -> "cosine", "norm" -> "none", "weight" -> Double.box(0.1), "filterBlacklistedContents" -> true.asInstanceOf[AnyRef])))
        val table1 = sc.cassandraTable[ContentRecos](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_RECOS).where("content_id=?", "domain_64106").first
        table1.scores.size should be(1)
        table1.scores.map(x => x._1).contains(table1.content_id) should be(false)
    }
    
    it should "save scores to cassandra without filtering blacklisted contents" in {
        
        populateDatabase();
        val me = EndOfContentRecommendationModel.execute(null, Option(Map("method" -> "cosine", "norm" -> "none", "weight" -> Double.box(0.1))))
        val table1 = sc.cassandraTable[ContentRecos](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_RECOS).where("content_id=?", "domain_64106").first
        table1.scores.size should be(2)
        table1.scores.map(x => x._1).contains(table1.content_id) should be(false)
    }
    
    it should "shouldn't throw any exception if blacklisted contents list is empty and filterBlacklistedContents is true " in {
        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            
            session.execute("TRUNCATE content_db.content_to_vector;");
            session.execute("TRUNCATE content_db.content_recos;");
            session.execute("TRUNCATE platform_db.recommendation_config");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_64106', [-0.002815, -0.00077, 0.00783, -0.003143, -0.008894, -0.003984, -0.001336, -0.005424, -0.000627, -0.000348, -0.000123, 0.009205, 0.003591, -0.001231, -0.008066] ,[-0.002815, -0.00077, 0.00783, -0.003143, -0.008894, -0.003984, -0.001336, -0.005424, -0.000627, -0.000348, -0.000123, 0.009205, 0.003591, -0.001231, -0.008066]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('org.ekstep.ms_52d02eae69702d0905cf0800', [-0.492884, 0.828462, 0.013562, -0.30689, -0.241799, -0.253693, 0.144239, 1.1541, -0.104697, 0.294227, 0.270508, -0.175924, -0.075452, -0.108453, 0.054697] ,[-0.000898, 6.214e-08, -0.010347, 0.014867, -0.013224, 0.979977, 0.003754, 0.011228, 3.1474e-06, 0.015441, 3.9e-05, -5e-05, 0.000514, 0.001222, -0.002526]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_48661', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_55065', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_70615', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('org.ekstep.aser', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
        }
        val me = EndOfContentRecommendationModel.execute(null, Option(Map("method" -> "cosine", "norm" -> "none", "weight" -> Double.box(0.1), "filterBlacklistedContents" -> true.asInstanceOf[AnyRef])))
        val table1 = sc.cassandraTable[ContentRecos](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_RECOS).where("content_id=?", "domain_64106").first
        table1.scores.size should be(2)
        table1.scores.map(x => x._1).contains(table1.content_id) should be(false)
    }
    
    def populateDatabase() {
        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            
            session.execute("TRUNCATE content_db.content_to_vector;");
            session.execute("TRUNCATE content_db.content_recos;");
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("TRUNCATE content_db.content_sideloading_summary;");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_64106', [-0.002815, -0.00077, 0.00783, -0.003143, -0.008894, -0.003984, -0.001336, -0.005424, -0.000627, -0.000348, -0.000123, 0.009205, 0.003591, -0.001231, -0.008066] ,[-0.002815, -0.00077, 0.00783, -0.003143, -0.008894, -0.003984, -0.001336, -0.005424, -0.000627, -0.000348, -0.000123, 0.009205, 0.003591, -0.001231, -0.008066]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('org.ekstep.ms_52d02eae69702d0905cf0800', [-0.492884, 0.828462, 0.013562, -0.30689, -0.241799, -0.253693, 0.144239, 1.1541, -0.104697, 0.294227, 0.270508, -0.175924, -0.075452, -0.108453, 0.054697] ,[-0.000898, 6.214e-08, -0.010347, 0.014867, -0.013224, 0.979977, 0.003754, 0.011228, 3.1474e-06, 0.015441, 3.9e-05, -5e-05, 0.000514, 0.001222, -0.002526]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_48661', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_55065', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_70615', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('org.ekstep.aser', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO platform_db.recommendation_config(config_key, config_value) VALUES ('content_reco_blacklist', ['org.ekstep.num.scrn.basic', 'do_30088866', 'numeracy_369', 'org.ekstep.aser', 'do_30088250', 'do_30014045', 'org.ekstep.delta', 'org.ekstep.esl1', 'do_30074519', 'domain_6444']);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_64106', 10, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'org.ekstep.ms_52d02eae69702d0905cf0800', 0.1, 0, 0, bigintAsBlob(3), 1459641600, 1459641600000, 1459641600000, 4, 5000, 0, 200);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_48661', 50, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 50, 0, 100);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_55065', 100, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 1000, 0, 120);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_70615', 0.5, 0, 0, bigintAsBlob(3), 1459641600, 1459641600000, 1459641600000, 4, 50000, 0, 10);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'org.ekstep.aser', 10, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 20, 0, 2);");
            session.execute("INSERT INTO content_db.content_sideloading_summary(content_id, num_downloads, total_count, num_sideloads, origin_map, avg_depth) VALUES ('domain_64106', 1000000, 15, 5, {}, 0.5);");
            session.execute("INSERT INTO content_db.content_sideloading_summary(content_id, num_downloads, total_count, num_sideloads, origin_map, avg_depth) VALUES ('org.ekstep.ms_52d02eae69702d0905cf0800', 50, 15, 5, {}, 0.5);");
            session.execute("INSERT INTO content_db.content_sideloading_summary(content_id, num_downloads, total_count, num_sideloads, origin_map, avg_depth) VALUES ('domain_48661', 100, 15, 5, {}, 0.5);");
            session.execute("INSERT INTO content_db.content_sideloading_summary(content_id, num_downloads, total_count, num_sideloads, origin_map, avg_depth) VALUES ('domain_55065', 1000, 15, 5, {}, 0.5);");
            session.execute("INSERT INTO content_db.content_sideloading_summary(content_id, num_downloads, total_count, num_sideloads, origin_map, avg_depth) VALUES ('domain_70615', 50000, 15, 5, {}, 0.5);");
            session.execute("INSERT INTO content_db.content_sideloading_summary(content_id, num_downloads, total_count, num_sideloads, origin_map, avg_depth) VALUES ('org.ekstep.aser', 1, 15, 5, {}, 0.5);");
        }
    }
}