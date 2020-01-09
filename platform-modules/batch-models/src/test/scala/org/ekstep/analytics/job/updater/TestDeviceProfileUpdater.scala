package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.EmbeddedPostgresql

class TestDeviceProfileUpdater extends SparkSpec(null) {

    val deviceTable = AppConf.getConfig("postgres.device.table_name")

    override def beforeAll(){
        super.beforeAll()
        EmbeddedPostgresql.start()
        EmbeddedPostgresql.execute(
            s"""
               |CREATE TABLE IF NOT EXISTS $deviceTable(
               |    device_id TEXT PRIMARY KEY,
               |    api_last_updated_on TIMESTAMP,
               |    avg_ts float,
               |    city TEXT,
               |    country TEXT,
               |    country_code TEXT,
               |    device_spec json,
               |    district_custom TEXT,
               |    fcm_token TEXT,
               |    first_access TIMESTAMP,
               |    last_access TIMESTAMP,
               |    producer_id TEXT,
               |    state TEXT,
               |    state_code TEXT,
               |    state_code_custom TEXT,
               |    state_custom TEXT,
               |    total_launches bigint,
               |    total_ts float,
               |    uaspec json,
               |    updated_date TIMESTAMP,
               |    user_declared_district TEXT,
               |    user_declared_state TEXT)""".stripMargin)

      EmbeddedPostgresql.execute(s"TRUNCATE $deviceTable")
      EmbeddedPostgresql.execute("INSERT INTO device_profile (device_id, first_access, last_access, total_ts, total_launches, avg_ts, state, city, device_spec, uaspec) VALUES ('48edda82418a1e916e9906a2fd7942cb', '2018-09-21 22:49:15.883', '2018-09-21 22:49:24.377', 18, 2, 9, 'Karnataka', 'Bangalore', '{\"os\":\"Android\",\"make\":\"Motorola XT1706\"}', '{\"raw\": \"xyz\"}');")
      EmbeddedPostgresql.execute(s"INSERT INTO $deviceTable (device_id, first_access, last_access, total_ts, total_launches, avg_ts) VALUES ('88edda82418a1e916e9906a2fd7942cb', '2018-09-20 22:49:15.883', '2018-09-22 19:39:41.139', 20, 2, 10);")
    }

    "DeviceProfileUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-profile/test-data1.log"))))), None, None, "org.ekstep.analytics.updater.DeviceProfileUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceProfileUpdater"), Option(false))
        DeviceProfileUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }


    override def afterAll() {
        super.afterAll()
        EmbeddedPostgresql.close()
    }
}