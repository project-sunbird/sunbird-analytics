package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.CommonUtil
import java.io.File
import org.joda.time.DateTime

class TestDeviceRecommendationTrainingModel extends SparkSpec(null) {

    "DeviceRecommendationTrainingModel" should "apply num_contents>5 filter, generate libsvm files, save all files in dataTime folder structure except feature file and save model to s3" in {

        populateDB();
        val dateTime = new DateTime()
        val date = dateTime.toLocalDate()
        val time = dateTime.toLocalTime().toString("HH-mm")
        val path_default = "/training/" + date + "/" + time + "/" 
        val me = DeviceRecommendationTrainingModel.execute(null, Option(Map("trainRatio" -> Double.box(1.0), "testRatio" -> Double.box(1.0), "dataLimit" -> Int.box(5), "libfm.executable_path" -> "src/test/resources/device-recos-training/", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "key" -> "model/test/", "model_name" -> "fm.model", "upload_model_s3" -> true.asInstanceOf[AnyRef], "saveFeatureFile" -> false.asInstanceOf[AnyRef], "dataTimeFolderStructure" -> true.asInstanceOf[AnyRef], "filterByNumContents" -> true.asInstanceOf[AnyRef])))
        val filePath = new File("src/test/resources/device-recos-training/RE-data/"+path_default)
        filePath.exists() should be (true)
        val featurefilePath = new File("src/test/resources/device-recos-training/RE-data/"+path_default+"featureDetails")
        featurefilePath.exists() should be (false)
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }
    
    it should "generate libsvm files for device aggregation, save all files in the given localPath and do not save model to s3" in {

        populateDB();
        val dateTime = new DateTime()
        val date = dateTime.toLocalDate()
        val time = dateTime.toLocalTime().toString("HH-mm")
        val path_default = "/training/" + date + "/" + time + "/" 
        val me = DeviceRecommendationTrainingModel.execute(null, Option(Map("trainRatio" -> Double.box(1.0), "testRatio" -> Double.box(1.0), "dataLimit" -> Int.box(5), "libfm.executable_path" -> "src/test/resources/device-recos-training/", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "key" -> "model/test/", "model_name" -> "fm.model", "upload_model_s3" -> false.asInstanceOf[AnyRef], "saveFeatureFile" -> true.asInstanceOf[AnyRef], "performAggregation" -> true.asInstanceOf[AnyRef], "dataTimeFolderStructure" -> false.asInstanceOf[AnyRef])))
        val filePath = new File("src/test/resources/device-recos-training/RE-data/"+path_default)
        filePath.exists() should be (false)
        val featurefilePath = new File("src/test/resources/device-recos-training/RE-data/"+"featureDetails")
        featurefilePath.exists() should be (true)
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }

    def populateDB() {
        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE device_db.device_usage_summary;");
            session.execute("TRUNCATE device_db.device_specification;");
            session.execute("TRUNCATE device_db.device_content_summary_fact;");
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 0.01, 0.07, 1475731808000, 'domain_68601', 1475731808000, 10, 0, 2, 410, 1, 1452038407000, 1475731808000, 3, 10, 30);");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 1.01, 10.07, 1452038407000, '', 1475731808000, 100, 10, 8, 10, 1, 1452038407000, 1475731808000, 300, 140, 300);");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 10.01, 1.07, 1475731808000, '', 1452038407000, 10, 0, 6, 410, 100, 1475731808000, 1452038407000, 3, 10, 30);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_68601', 0.1, 1452038407000, false, null, 1475731808000, 0, 0, 1, 1, 1475731808000, 10, 10);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_63844', null, 1475731808000, true, null, 1452038407000, 1, 10, 20, 100, 1452038407000, 1534, 1234);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 'domain_68601', 10.0, 1475731808000, false, null, 1452038407000, 0, 0, 1, 1, 1475731808000, 20, 20);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 'domain_63844', 2.0, 1452038407000, true, null, 1475731808000, 1, 10, 20, 100, 1459641600000, 1534, 124);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc48a084', 'domain_63333', 100.0, 1475731808000, false, null, 1452038407000, 0, 0, 1, 1, 1452038407000, 10, 20);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 'domain_70615', null, 1452038407000, true, null, 1475731808000, 1, 10, 20, 100, 1459641600000, 1534, 124);");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'Android 4.4.2', 3.89, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Micromax Micromax A065', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 'Android 5.0.1', 5.7, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Samsung S685', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 'Android 5.0.1', 5.7, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Samsung S685', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, d_app_id, d_channel_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_63844', 'Genie', 'Ekstep', 0, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, d_app_id, d_channel_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_68601', 'Genie', 'Ekstep', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600000, 1459641600000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, d_app_id, d_channel_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63844', 'Genie', 'Ekstep', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
        }
    }
}