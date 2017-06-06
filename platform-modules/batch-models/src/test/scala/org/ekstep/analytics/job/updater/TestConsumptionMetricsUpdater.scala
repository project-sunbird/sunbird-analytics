package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.util.Constants
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import com.datastax.spark.connector.cql.CassandraConnector
import java.net.URI
import com.pygmalios.reactiveinflux._
import org.joda.time.DateTime
import scala.concurrent.duration._
import org.joda.time.DateTimeUtils

class TestConsumptionMetricsUpdater extends SparkSpec(null) {

    override def beforeAll() {
        super.beforeAll()
        DateTimeUtils.setCurrentMillisFixed(1487788200000L);
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        session.execute("TRUNCATE content_db.content_usage_summary_fact;");
        session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2017701, 'all' ,'all', 4, 5, 6, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 6, 10, 20);");
        session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2016731, 'all' ,'all', 5, 6, 8, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 55, 10, 20);");
        session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (2017702, 'all' ,'all', 4, 66, 7, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 4, 7, 20);");
        session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (20170222, 'all' ,'all', 4, 6, 11, bigintAsBlob(3), 1459641600, 1475731808000, 1475731808000, 4, 8, 9, 20);");
        session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (20170221, 'all' ,'all', 1, 2, 3, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 5, 7, 20);");
        session.execute("INSERT INTO content_db.content_usage_summary_fact(d_period, d_tag, d_content_id, m_avg_interactions_min, m_avg_sess_device, m_avg_ts_session, m_device_ids, m_last_gen_date, m_last_sync_date, m_publish_date, m_total_devices, m_total_interactions, m_total_sessions, m_total_ts) VALUES (201611, 'all' ,'all', 3, 4, 6, bigintAsBlob(3), 1459641600, 1452038407000, 1452038407000, 4, 3, 3, 20);");
        session.execute("TRUNCATE content_db.genie_launch_summary_fact;");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (2017701, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (2017702, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (2017703, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (2017704, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (2017705, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (2017706, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (20170222, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");
        session.execute("INSERT INTO content_db.genie_launch_summary_fact(d_period, d_tag, m_avg_sess_device, m_avg_ts_session, m_contents, m_device_ids, m_total_devices, m_total_sessions, m_total_ts) VALUES (20170221, 'all', 4, 5, ['sam'], bigintAsBlob(4), 6, 10, 22);");

        val config = JobConfig(Fetcher("local", None, None), None, None, "org.ekstep.analytics.updater.ConsumptionMetricsUpdater", Option(Map("periodType" -> "WEEK", "periodUpTo" -> 100.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Consumption Metrics Updater"), Option(false))
        val strConfig = JSONUtils.serialize(config);
        ConsumptionMetricsUpdater.main(strConfig)(Option(sc));
    }

    "ConsumptionMetricsUpdater" should "push data into influxDB" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM content_metrics")
            queryResult.result.isEmpty should be(false)
        }
    }

    it should "check count of coulmns in fluxdb table" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM content_metrics where time=1483833600000000000")
            queryResult.result.singleSeries.columns.size should be(9)
        }
    }

    it should "validate table name" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM content_metrics")
            queryResult.result.singleSeries.name should be("content_metrics")
        }
    }

    it should "generate first coulmn as time " in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM content_metrics")
            queryResult.result.singleSeries.columns(0) should be("time")
        }
    }

    ignore should "validate timespent for period in content_metrics" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT timespent FROM content_metrics where period = 'week' and time = 1470528000000000000")
            queryResult.row.mkString.split(",")(1).trim() should be("20")
        }
    }

    it should "compute content usage / content visits and store in influx" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM genie_stats where period = 'week' AND time = 1483833600000000000")
            queryResult.row.mkString.split(",")(1).trim() should be("2")
        }
    }

    it should "compute content visits / genie visits and store in influx" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM genie_stats where period = 'week' AND time = 1483833600000000000")
            queryResult.row.mkString.split(",")(3).trim() should be("1")
        }
    }

    it should "compute genie visits / devices and store in influx" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM genie_stats where period = 'week'AND time = 1483833600000000000")
            queryResult.row.mkString.split(",")(5).trim() should be("1.6666666666666667")
        }
    }

    it should "compute content usage / device and store in influx" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM genie_stats where period = 'week'AND time = 1483833600000000000")
            queryResult.row.mkString.split(",")(2).trim() should be("3.3333333333333335")
        }
    }

}