package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.EmbeddedPostgresql

class TestDeviceSummarizer extends SparkSpec(null) {

    val deviceTable = AppConf.getConfig("postgres.device.table_name")

    override def beforeAll(){
        super.beforeAll()
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
    }

    "DeviceSummarizer" should "execute DeviceSummarizer job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.DeviceSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceSummarizer"))
        DeviceSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}