package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher

class TestEndOfContentRecommendationJob extends SparkSpec(null) {
  
    "EndOfContentRecommendationJob" should "execute the job and shouldn't throw any exception" in {
        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            
            session.execute("TRUNCATE local_content_db.content_to_vector;");
            session.execute("TRUNCATE local_content_db.content_recos;");
            session.execute("TRUNCATE local_platform_db.recommendation_config");
            session.execute("INSERT INTO local_content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_64106', [-0.002815, -0.00077, 0.00783, -0.003143, -0.008894, -0.003984, -0.001336, -0.005424, -0.000627, -0.000348, -0.000123, 0.009205, 0.003591, -0.001231, -0.008066] ,[-0.002815, -0.00077, 0.00783, -0.003143, -0.008894, -0.003984, -0.001336, -0.005424, -0.000627, -0.000348, -0.000123, 0.009205, 0.003591, -0.001231, -0.008066]);");
            session.execute("INSERT INTO local_content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('org.ekstep.ms_52d02eae69702d0905cf0800', [-0.492884, 0.828462, 0.013562, -0.30689, -0.241799, -0.253693, 0.144239, 1.1541, -0.104697, 0.294227, 0.270508, -0.175924, -0.075452, -0.108453, 0.054697] ,[-0.000898, 6.214e-08, -0.010347, 0.014867, -0.013224, 0.979977, 0.003754, 0.011228, 3.1474e-06, 0.015441, 3.9e-05, -5e-05, 0.000514, 0.001222, -0.002526]);");
            session.execute("INSERT INTO local_content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_48661', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO local_content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_55065', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO local_content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('domain_70615', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO local_content_db.content_to_vector(content_id, tag_vec, text_vec) VALUES ('org.ekstep.aser', [-0.151077, -0.038196, 1.14139, 0.04051, -0.000513, -0.004798, -0.001534, -0.000982, 0.000553, 0.002322, 0.00144, -0.001178, -0.000181, 0.007115, 0.005131] ,[1.05487, -2.0419e-08, -0.096328, 0.037501, 0.002662, -0.000563, 0.000671, 0.001206, -7.6287e-07, -0.000422, 2.3701e-06, -2.2e-05, 0.000516, 2.9e-05, -0.000125]);");
            session.execute("INSERT INTO local_platform_db.recommendation_config(config_key, config_value) VALUES ('content_reco_blacklist', ['org.ekstep.num.scrn.basic', 'do_30088866', 'numeracy_369', 'org.ekstep.aser', 'do_30088250', 'do_30014045', 'org.ekstep.delta', 'org.ekstep.esl1', 'do_30074519', 'domain_6444']);");
        }
        
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), None, None, "org.ekstep.analytics.model.EndOfContentRecommendationModel", Option(Map("method" -> "cosine", "norm" -> "none", "weight" -> Double.box(0.1))), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestEndOfContentRecommendationJob"), Option(false))
        EndOfContentRecommendationJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}