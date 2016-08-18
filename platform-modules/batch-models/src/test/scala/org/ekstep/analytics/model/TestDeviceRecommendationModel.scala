package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector

class TestDeviceRecommendationModel extends SparkSpec(null) {
  
    ignore should "generate libsvm files and generate scores" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE device_db.device_usage_summary;");
            session.execute("TRUNCATE device_db.device_specification;");
            session.execute("TRUNCATE device_db.device_content_summary_fact;");
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 0.01, 0.07, 1459641600, 'domain_68601', 1461715199, 10, 0, 2, 410, 1, 1459641600, 1459641600, 3, 10, 30);");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 0.01, 0.07, 1459641600, '', 1461715199, 10, 0, 2, 410, 1, 1459641600, 1459641600, 3, 10, 30);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_68601', null, 1459641600, false, null, 1461715199, 0, 0, 1, 1, 1459641600, 10, 10);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_63844', null, 1459641600, true, null, 1461715199, 1, 10, 20, 100, 1459641600, 1534, 1234);");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'Android 4.4.2', 3.89, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Micromax Micromax A065', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 'Android 5.0.1', 5.7, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Samsung S685', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 'Android 5.0.1', 5.7, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Samsung S685', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_content_id, d_period, d_group_user, d_content_type, d_mime_type, m_avg_interactions_min, m_avg_sessions_week, m_avg_ts_session, m_avg_ts_week, m_last_sync_date, m_publish_date, m_total_interactions, m_total_sessions, m_total_ts) VALUES ('domain_68601', 0, true, 'Worksheet', 'application/vnd.ekstep.ecml-archive', 25.62, 10, 65.118, 651.18, 1459641600, 1459641600, 278, 10, 651.18);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_content_id, d_period, d_group_user, d_content_type, d_mime_type, m_avg_interactions_min, m_avg_sessions_week, m_avg_ts_session, m_avg_ts_week, m_last_sync_date, m_publish_date, m_total_interactions, m_total_sessions, m_total_ts) VALUES ('domain_63844', 0, true, 'Worksheet', 'application/vnd.ekstep.ecml-archive', 25.62, 2, 65.12, 651.18, 1459641600, 1459641600, 139, 10, 65.12);");
        }
        
        val me = DeviceRecommendationModel.execute(null, None);
        val res = me.collect()
        res.length should be(3)
        
        val row1 = res(0)
        row1.device_id should be ("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036")
        row1.scores.length should be (656)
    }

}