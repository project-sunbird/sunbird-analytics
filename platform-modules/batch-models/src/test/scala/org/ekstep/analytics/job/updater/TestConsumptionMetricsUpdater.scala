package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import com.paulgoldbaum.influxdbclient.InfluxDB
import scala.concurrent.Await
import org.ekstep.analytics.util.Constants
import com.paulgoldbaum.influxdbclient._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.datastax.spark.connector.cql.CassandraConnector
import com.paulgoldbaum.influxdbclient.InfluxDB
import org.ekstep.analytics.framework.conf.AppConf

class TestConsumptionMetricsUpdater extends SparkSpec(null) {
	"ConsumptionMetricsUpdater" should "execute the job" in {
		val config = JobConfig(Fetcher("local", None, None), None, None, "org.ekstep.analytics.updater.ConsumptionMetricsUpdater", Option(Map("periodType"-> "ALL", "periodUpTo" -> 10.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Consumption Metrics Updater"), Option(false))
		val strConfig = JSONUtils.serialize(config);
		println(strConfig);
		ConsumptionMetricsUpdater.main(strConfig)(Option(sc));
    }
    
    it should "check the database columns" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_68601', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600000, 1459641600000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2017701, '42d3b7edc2e9b59a286b1956e3cdbc492706ac21' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2017702, '42d3b7edc2e9b59a286b1956e3cdbc492706ac21' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (20170329, '42d3b7edc2e9b59a286b1956e3cdbc492706ac21' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (20170223, 'all' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (201611, 'all' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 0, 0, 20);");
        }
        val influxdb = InfluxDB.connect(AppConf.getConfig("reactiveinflux.host"), AppConf.getConfig("reactiveinflux.port").toInt)
        val database = influxdb.selectDatabase(AppConf.getConfig("reactiveinflux.database"))
        val result = database.query("SELECT * FROM content_metrics")
        val res = Await.result(result, 5 seconds)
        res.series.head.columns.size should be(7)
    }
}