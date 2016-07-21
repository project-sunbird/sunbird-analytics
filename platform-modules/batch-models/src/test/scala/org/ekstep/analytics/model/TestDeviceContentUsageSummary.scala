package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._

class TestDeviceContentUsageSummary extends SparkSpec(null) {

    it should "generate device content summary events" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE learner_db.device_content_summary");
            session.execute("TRUNCATE learner_db.device_usage_summary;");
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

        events2.length should be(5)
        val event2 = events2(0);

        event2.eid should be("ME_DEVICE_CONTENT_USAGE_SUMMARY");
        event2.context.pdata.model should be("DeviceContentUsageSummary");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("CUMULATIVE");
        event2.dimensions.did.get should be("6b5186b698e30e62b4742dddf56984b3a19e9520");
        event2.dimensions.gdata.get.id should be("numeracy_369");
        event2.context.date_range should not be null;

        val eks2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks2.get("last_played_on").get should be(1462869647260L)
        eks2.get("total_timespent").get should be(25)
        eks2.get("avg_interactions_min").get should be(24)
        eks2.get("mean_play_time_interval").get should be(0)

        val event3 = events2(1);

        event3.eid should be("ME_DEVICE_CONTENT_USAGE_SUMMARY");
        event3.context.pdata.model should be("DeviceContentUsageSummary");
        event3.context.pdata.ver should be("1.0");
        event3.context.granularity should be("CUMULATIVE");
        event3.dimensions.did.get should be("6b5186b698e30e62b4742dddf56984b3a19e9520");
        event3.dimensions.gdata.get.id should be("numeracy_360");
        event3.context.date_range should not be null;

        val eks3 = event3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks3.get("last_played_on").get should be(1462869647260L)
        eks3.get("total_timespent").get should be(35)
        eks3.get("avg_interactions_min").get should be(34.29)
        eks3.get("mean_play_time_interval").get should be(2141937.63)

        val table1 = sc.cassandraTable[UsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).where("device_id=?", "0b303d4d66d13ad0944416780e52cc3db1feba87").first
        table1.avg_num_launches should be(0)
        table1.avg_time should be(0)
        table1.num_days should be(0)
        table1.num_contents should be(0)
        table1.play_start_time should be(1460627674628L)
        table1.last_played_on should be(1461669647260L)
        table1.total_play_time should be(30)
        table1.num_sessions should be(3)
        table1.mean_play_time should be(10)
        table1.mean_play_time_interval should be(520971.32)
        table1.previously_played_content should be("numeracy_369")

        val rdd5 = loadFile[DerivedEvent]("src/test/resources/device-usage-summary/test_data_1.log");
        val rdd6 = DeviceUsageSummary.execute(rdd5, Option(Map("modelId" -> "DeviceUsageSummarizer", "granularity" -> "DAY")));

        val table2 = sc.cassandraTable[UsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).where("device_id=?", "0b303d4d66d13ad0944416780e52cc3db1feba87").first
        table2.avg_num_launches should be(0.25)
        table2.avg_time should be(2.5)
        table2.start_time should be(1460627512768L)
        table2.end_time should be(1461669647260L)
        table2.num_days should be(12)
        table2.num_contents should be(0)
        table2.play_start_time should be(1460627674628L)
        table2.last_played_on should be(1461669647260L)
        table2.total_play_time should be(30)
        table2.num_sessions should be(3)
        table2.mean_play_time should be(10)
        table2.mean_play_time_interval should be(520971.32)
        table2.previously_played_content should be("numeracy_369")

    }
}