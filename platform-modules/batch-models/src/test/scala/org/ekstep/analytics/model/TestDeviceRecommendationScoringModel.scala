package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.ml.linalg.{ Vector, Vectors, DenseVector }
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.adapter.ContentModel
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.updater.DeviceSpec
import org.ekstep.analytics.job.summarizer.DeviceRecommendationTrainingJob
import java.io.File
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.adapter.ContentAdapter

class TestDeviceRecommendationScoringModel extends SparkSpec(null) {
    
    ignore should "load model with zero pairwise interactions and generate scores" in {

        val dateTime = new DateTime()
        val date = dateTime.toLocalDate()
        val time = dateTime.toLocalTime().toString("HH-mm")
        val path_default = "/scoring/" + date + "/" + time + "/" 
        REScoringMock.execute(null, Option(Map("model_name" -> "fm.model1", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "dataTimeFolderStructure" -> true.asInstanceOf[AnyRef], "key" -> "model/test/", "saveScoresToFile" -> true.asInstanceOf[AnyRef])))
        val filePath = new File("src/test/resources/device-recos-training/RE-data/"+path_default)
        filePath.exists() should be (true)
        val scoresfilePath = new File("src/test/resources/device-recos-training/RE-data/"+path_default+"RE-score")
        scoresfilePath.exists() should be (true)
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }

    ignore should "load model with pairwise interactions and generate scores" in {

        val dateTime = new DateTime()
        val date = dateTime.toLocalDate()
        val time = dateTime.toLocalTime().toString("HH-mm")
        val path_default = "/scoring/" + date + "/" + time + "/" 
        REScoringMock.execute(null, Option(Map("model_name" -> "fm.model2", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "dataTimeFolderStructure" -> false.asInstanceOf[AnyRef], "key" -> "model/test/", "saveScoresToFile" -> false.asInstanceOf[AnyRef])))
        val filePath = new File("src/test/resources/device-recos-training/RE-data/"+path_default)
        filePath.exists() should be (false)
        val scoresfilePath = new File("src/test/resources/device-recos-training/RE-data/"+"RE-score")
        scoresfilePath.exists() should be (false)
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }

    ignore should "load model with zero W0, generate scores and check for filter contents already in the device & for filter blacklisted contents" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            
            session.execute("TRUNCATE platform_db.recommendation_config");
            session.execute("INSERT INTO platform_db.recommendation_config(config_key, config_value) VALUES ('device_reco_blacklist', ['Test_QAT_1070']);");
        }
        REScoringMock.execute(null, Option(Map("model_name" -> "fm.model3", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "dataTimeFolderStructure" -> false.asInstanceOf[AnyRef], "key" -> "model/test/", "filterBlacklistedContents" -> true.asInstanceOf[AnyRef])))
        val table1 = sc.cassandraTable[DeviceRecos](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_RECOS).where("device_id=?", "9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044").first
        table1.scores.size should be(0)
        table1.scores.map{x => x._1}.contains("Test_QAT_1070") should be(false)
        val table2 = sc.cassandraTable[DeviceRecos](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_RECOS).where("device_id=?", "9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043").first
        table2.scores.size should be(1)
        table2.scores.map{x => x._1}.contains("Test_QAT_1070") should be(false)
        table2.scores.map{x => x._1}.contains("domain_49024") should be(true)
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }

    ignore should "load model with zero unary interactions, generate scores" in {

        REScoringMock.execute(null, Option(Map("model_name" -> "fm.model4", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "dataTimeFolderStructure" -> false.asInstanceOf[AnyRef], "key" -> "model/test/", "filterBlacklistedContents" -> false.asInstanceOf[AnyRef])))
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }
    
    it should "check preprocess method without filtering by num_contents and save json input data" in {

        populateDB();
        val num_contents = ContentAdapter.getPublishedContentForRE().map { x => (x.id, x) }.toMap.size
        val data = DeviceRecommendationScoringModel.preProcess(null, Map("model_name" -> "fm.model4", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "saveInputData"->true.asInstanceOf[AnyRef], "dataTimeFolderStructure" -> false.asInstanceOf[AnyRef], "key" -> "model/test/", "filterByNumContents" -> false.asInstanceOf[AnyRef]));
        data.count() should be (num_contents*3)
        val inputfilePath = new File("src/test/resources/device-recos-training/RE-data/"+"RE-input")
        inputfilePath.exists() should be (true)
        val out = sc.textFile("src/test/resources/device-recos-training/RE-data/"+"RE-input")
        out.count() should be(num_contents*3)
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }
    
    it should "check preprocess method with filtering by num_contents and save json input data" in {

        populateDB();
        val num_contents = ContentAdapter.getPublishedContentForRE().map { x => (x.id, x) }.toMap.size
        val data = DeviceRecommendationScoringModel.preProcess(null, Map("model_name" -> "fm.model4", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "saveInputData"->true.asInstanceOf[AnyRef], "dataTimeFolderStructure" -> false.asInstanceOf[AnyRef], "key" -> "model/test/", "filterByNumContents" -> true.asInstanceOf[AnyRef]));
        data.count() should be (num_contents*2)
        val inputfilePath = new File("src/test/resources/device-recos-training/RE-data/"+"RE-input")
        inputfilePath.exists() should be (true)
        val out = sc.textFile("src/test/resources/device-recos-training/RE-data/"+"RE-input")
        out.count() should be(num_contents*2)
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }
    
    def populateDB() {
        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE device_db.device_usage_summary;");
            session.execute("TRUNCATE device_db.device_specification;");
            session.execute("TRUNCATE device_db.device_content_summary_fact;");
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 0.01, 0.07, 1475731808000, 'domain_68601', 1475731808000, 10, 0, 2, 410, 1, 1452038407000, 1475731808000, 3, 10, 30);");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 0.01, 0.07, 1475731808000, '', 1452038407000, 10, 0, 8, 410, 1, 1475731808000, 1452038407000, 3, 10, 30);");
            session.execute("INSERT INTO device_db.device_usage_summary(device_id, avg_num_launches, avg_time, end_time, last_played_content, last_played_on, mean_play_time, mean_play_time_interval, num_contents, num_days, num_sessions, play_start_time, start_time, total_launches, total_play_time, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 0.01, 0.07, 1475731808000, '', 1452038407000, 10, 0, 6, 410, 1, 1475731808000, 1452038407000, 3, 10, 30);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_68601', null, 1452038407000, false, null, 1475731808000, 0, 0, 1, 1, 1459641600000, 10, 10);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_63844', null, 1475731808000, true, null, 1452038407000, 1, 10, 20, 100, 1452038407000, 1534, 1234);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 'domain_68601', null, 1475731808000, false, null, 1452038407000, 0, 0, 1, 1, 1459641600000, 10, 20);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 'domain_63844', null, 1452038407000, true, null, 1475731808000, 1, 10, 20, 100, 1459641600000, 1534, 124);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc48a084', 'domain_63333', null, 1475731808000, false, null, 1452038407000, 0, 0, 1, 1, 1459641600000, 10, 20);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 'domain_70615', null, 1452038407000, true, null, 1475731808000, 1, 10, 20, 100, 1459641600000, 1534, 124);");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'Android 4.4.2', 3.89, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Micromax Micromax A065', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043', 'Android 5.0.1', 5.7, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Samsung S685', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO device_db.device_specification(device_id, os, screen_size, capabilities, cpu, device_local_name, device_name, external_disk, internal_disk, make, memory, num_sims, primary_secondary_camera) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044', 'Android 5.0.1', 5.7, [''], 'abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)', '', '', 1.13, 835.78, 'Samsung S685', -1, 1, '5.0,1.0');");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_68601', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600000, 1459641600000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
        }
    }
    
}

object REScoringMock {
    
    val x = List(DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_49024",ContentModel("domain_49024",List("literacy"),"Story",List("English")),ContentToVector("domain_49024", None, None)
            ,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_49024",None,None,None,None,Option(0.0),None,None,None,None,None,None,None),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),"domain_68601",
            ContentModel("domain_68601",List("numeracy"),"Worksheet",List("Hindi")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_68601",None,Option(10),Option(20),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(false),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","","","Android 5.0.1","Samsung S685",-1.0,1.0,1.0,1.0,"1.0,1.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),dus_tf(Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(0.0),Option(1.0),Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),
            dcus_tf("domain_68601",Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),null),
            DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","Test_QAT_1070",ContentModel("Test_QAT_1070",List("numeracy"),"Worksheet",List("Kannada")),null,
            DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","Test_QAT_1070",None,None,None,None,Option(10.0),None,None,None,None,None,None,None),cus_t(Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),"domain_68601",
            ContentModel("domain_68602",List("literacy"),"Story",List("English")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_68601",None,Option(1),Option(10),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(true),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","","","Android 5.0.1","Samsung S685",-1.0,1.0,1.0,1.0,"1.0,1.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),dus_tf(Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),
            dcus_tf("domain_68601",Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),null),
            DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","domain_49024",ContentModel("domain_49024",List("literacy"),"Story",List("English")),ContentToVector("domain_49024", None, None)
            ,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","domain_49024",None,None,None,None,Option(20.0),None,None,None,None,None,None,None),cus_t(None,None,None,None,None,None,None,None,None),"domain_68601",
            ContentModel("domain_68601",List("numeracy"),"Worksheet",List("Hindi")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","domain_68601",None,Option(1),Option(10),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(false),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","","","Android 5.0.1","Micromax A065",-1.0,2.0,2.0,2.0,"2.0,2.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),dus_tf(Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(0.0),Option(1.0),Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),
            dcus_tf(null,None,None,None,None,None,None,None,None,None,None),null),
            DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","Test_QAT_1070",ContentModel("Test_QAT_1070",List("numeracy"),"Worksheet",List("Kannada")),null,
            DeviceContentSummary(null,null,None,None,None,None,None,None,None,None,None,None,None,None),cus_t(None,None,None,None,None,None,None,None,None),"domain_68601",
            ContentModel("domain_68602",List("literacy"),"Story",List("English")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","domain_68601",None,Option(1),Option(10),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(false),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","","","Android 5.0.1","Micromax A065",-1.0,2.0,2.0,2.0,"2.0,2.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),dus_tf(Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(0.0),Option(1.0),Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),
            dcus_tf(null,None,None,None,None,None,None,None,None,None,None),null)) 
            
    def preProcess()(implicit sc: SparkContext): RDD[DeviceContext] = {
        sc.makeRDD(x);
    }
    def execute(events: RDD[DerivedEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[DeviceRecos] = {
        
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val inputRDD = preProcess();
        JobContext.recordRDD(inputRDD);
        val outputRDD = DeviceRecommendationScoringModel.algorithm(inputRDD, config);
        JobContext.recordRDD(outputRDD);
        val resultRDD = DeviceRecommendationScoringModel.postProcess(outputRDD, config);
        JobContext.recordRDD(resultRDD);
        resultRDD
    }
}