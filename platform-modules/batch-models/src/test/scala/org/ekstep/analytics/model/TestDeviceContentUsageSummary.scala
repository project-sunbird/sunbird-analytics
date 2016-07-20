package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector

class TestDeviceContentUsageSummary extends SparkSpec(null) {

    it should "generate device content summary events" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("truncate learner_db.device_content_summary");
        }

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/device-content-usage-summary/test_data1.log");
        val rdd2 = DeviceContentUsageSummary.execute(rdd1, None);
        val events1 = rdd2.collect

        events1.length should be(3)
        val event1 = events1(0);

        event1.eid should be("ME_DEVICE_CONTENT_USAGE_SUMMARY");
        event1.context.pdata.model should be("DeviceContentUsageSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("CUMULATIVE");
        event1.dimensions.did.get should be("6b5186b698e30e62b4742dddf56984b3a19e9520");
        event1.dimensions.gdata.get.id should be("numeracy_360");
        event1.context.date_range should not be null;

        val eks1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks1.get("last_played_on").get should be(1461669647260L)
        eks1.get("total_timespent").get should be(10)
        eks1.get("avg_interactions_min").get should be(60)
        eks1.get("mean_play_time_interval").get should be(0)
        
        val rdd3 = loadFile[DerivedEvent]("src/test/resources/device-content-usage-summary/test_data2.log");
        val rdd4 = DeviceContentUsageSummary.execute(rdd3, None);
        val events2 = rdd4.collect

        events2.length should be(3)
        val event2 = events2(0);

        event2.eid should be("ME_DEVICE_CONTENT_USAGE_SUMMARY");
        event2.context.pdata.model should be("DeviceContentUsageSummary");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("CUMULATIVE");
        event2.dimensions.did.get should be("6b5186b698e30e62b4742dddf56984b3a19e9520");
        event2.dimensions.gdata.get.id should be("numeracy_369");
        event2.context.date_range should not be null;

        val eks2 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks2.get("last_played_on").get should be(1461669647260L)
        eks2.get("total_timespent").get should be(10)
        eks2.get("avg_interactions_min").get should be(60)
        eks2.get("mean_play_time_interval").get should be(0)
    }
}