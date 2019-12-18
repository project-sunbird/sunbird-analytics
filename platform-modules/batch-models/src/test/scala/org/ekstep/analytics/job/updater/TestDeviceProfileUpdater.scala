package org.ekstep.analytics.job.updater

import java.sql.Statement

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.model.SparkSpec

class TestDeviceProfileUpdater extends SparkSpec(null) {

    val deviceTable = AppConf.getConfig("postgres.device.table_name")
    val pg: EmbeddedPostgres = EmbeddedPostgres.builder().setPort(65124).start()
    val connection = pg.getPostgresDatabase().getConnection()
    val stmt: Statement = connection.createStatement()


    override def beforeAll(){
        super.beforeAll()
        stmt.execute(
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
    }

    override def afterAll(): Unit = {
        pg.close()
    }
  
    "DeviceProfileUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-profile/test-data1.log"))))), None, None, "org.ekstep.analytics.updater.DeviceProfileUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceProfileUpdater"), Option(false))
        DeviceProfileUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}