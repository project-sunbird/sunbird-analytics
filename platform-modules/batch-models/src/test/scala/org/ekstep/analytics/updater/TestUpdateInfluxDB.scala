package org.ekstep.analytics.updater
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import com.paulgoldbaum.influxdbclient.InfluxDB
import scala.concurrent.Await
import org.ekstep.analytics.util.Constants
import com.paulgoldbaum.influxdbclient._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.datastax.spark.connector.cql.CassandraConnector

class TestUpdateInfluxDB extends SparkSpec(null) {
    "UpdateInfluxDB" should "update the influxdb" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.content_usage_summary_fact;");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (0, 'all' ,'domain_68601', 0, 0, 0, bigintAsBlob(3), 1459641600, 1459641600000, 1459641600000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2017701, 'dff9175fa217e728d86bc1f4d8f818f6d2959303' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
            session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2017702, '42d3b7edc2e9b59a286b1956e3cdbc492706ac21' ,'domain_63844', 0, 0, 0, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 0, 0, 20);");
        }
        val rdd = loadFile[Items]("src/test/resources/influxDB-updater/item-metrics.json");
        val rdd2 = UpdateInfluxDB.execute(rdd, None);
        val influxdb = InfluxDB.connect(Constants.LOCAL_HOST, Constants.INFLUX_DB_PORT.toInt)
        val database = influxdb.selectDatabase(Constants.INFLUX_DB_NAME)
        val result = database.query("SELECT * FROM content_metrics")
        val res = Await.result(result, 5 seconds)
        res.series.head.columns(0) should be("time")
    }
}