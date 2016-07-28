package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._

class TestDeviceContentUsageSummary extends SparkSpec(null) {

    it should "generate device content summary events" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE learner_db.device_content_summary_fact");
            session.execute("TRUNCATE learner_db.device_usage_summary;");
        }

        val rdd = loadFile[Event]("src/test/resources/device-content-usage-summary/telemetry_test_data.log");
        val me = ContentSideloadingSummary.execute(rdd, None);

        val table1 = sc.cassandraTable[DeviceUsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).where("device_id=?", "0b303d4d66d13ad0944416780e52cc3db1feba87").first
        table1.avg_num_launches should be(None)
        table1.avg_time should be(None)
        table1.num_days should be(None)
        table1.num_contents.get should be(2)
        table1.play_start_time should be(None)
        table1.last_played_on should be(None)
        table1.total_play_time should be(None)
        table1.num_sessions should be(None)
        table1.mean_play_time should be(None)
        table1.mean_play_time_interval should be(None)
        table1.last_played_content should be(None)

        val table2 = sc.cassandraTable[DeviceContentSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).where("device_id=?", "0b303d4d66d13ad0944416780e52cc3db1feba87").first
        table2.content_id should be("numeracy_360")
        table2.avg_interactions_min should be(None)
        table2.downloaded.get should be(false)
        table2.download_date.get should be(1459839946698L)
        table2.last_played_on should be(None)
        table2.mean_play_time_interval should be(None)
        table2.num_group_user should be(None)
        table2.num_individual_user should be(None)
        table2.num_sessions should be(None)
        table2.start_time should be(None)
        table2.total_interactions should be(None)
        table2.total_timespent should be(None)

        val rdd0 = loadFile[Event]("src/test/resources/device-content-usage-summary/telemetry_test_data2.log");
        val me1 = ContentSideloadingSummary.execute(rdd0, None);
        
        val table = sc.cassandraTable[DeviceUsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).where("device_id=?", "0b303d4d66d13ad0944416780e52cc3db1feba87").first
        table.num_contents.get should be(4)
        
        val rdd1 = loadFile[DerivedEvent]("src/test/resources/device-content-usage-summary/test_data1.log");
        val rdd2 = DeviceContentUsageSummary.execute(rdd1, None);
        val events1 = rdd2.collect

        events1.length should be(4)
        val event1 = events1(0);

        event1.eid should be("ME_DEVICE_CONTENT_USAGE_SUMMARY");
        event1.context.pdata.model should be("DeviceContentUsageSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("CUMULATIVE");
        event1.dimensions.did.get should be("6b5186b698e30e62b4742dddf56984b3a19e9520");
        event1.dimensions.gdata.get.id should be("numeracy_360");
        event1.context.date_range should not be null;

        val eks1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks1.get("last_played_on").get should be(Some(1461669647260L))
        eks1.get("total_timespent").get should be(Some(10))
        eks1.get("avg_interactions_min").get should be(Some(60))
        eks1.get("mean_play_time_interval").get should be(Some(0))
        eks1.get("num_group_user").get should be(Some(0))
        eks1.get("num_individual_user").get should be(Some(1))

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
        eks2.get("last_played_on").get should be(Some(1462869647260L))
        eks2.get("total_timespent").get should be(Some(25))
        eks2.get("avg_interactions_min").get should be(Some(24))
        eks2.get("mean_play_time_interval").get should be(Some(0))
        eks2.get("num_group_user").get should be(Some(1))
        eks2.get("num_individual_user").get should be(Some(0))

        val event3 = events2(1);

        event3.eid should be("ME_DEVICE_CONTENT_USAGE_SUMMARY");
        event3.context.pdata.model should be("DeviceContentUsageSummary");
        event3.context.pdata.ver should be("1.0");
        event3.context.granularity should be("CUMULATIVE");
        event3.dimensions.did.get should be("6b5186b698e30e62b4742dddf56984b3a19e9520");
        event3.dimensions.gdata.get.id should be("numeracy_360");
        event3.context.date_range should not be null;

        val eks3 = event3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks3.get("last_played_on").get should be(Some(1462869647260L))
        eks3.get("total_timespent").get should be(Some(35))
        eks3.get("avg_interactions_min").get should be(Some(34.29))
        eks3.get("mean_play_time_interval").get should be(Some(2141937.63))

        val table3 = sc.cassandraTable[DeviceUsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).where("device_id=?", "0b303d4d66d13ad0944416780e52cc3db1feba87").first
        table3.avg_num_launches should be(None)
        table3.avg_time should be(None)
        table3.num_days should be(None)
        table3.num_contents.get should be(4)
        table3.play_start_time.get should be(1460627674628L)
        table3.last_played_on.get should be(1461669647260L)
        table3.total_play_time.get should be(30)
        table3.num_sessions.get should be(3)
        table3.mean_play_time.get should be(10)
        table3.mean_play_time_interval.get should be(520971.32)
        table3.last_played_content.get should be("numeracy_369")

        val rdd5 = loadFile[DerivedEvent]("src/test/resources/device-usage-summary/test_data_1.log");
        val rdd6 = DeviceUsageSummaryModel.execute(rdd5, Option(Map("modelId" -> "DeviceUsageSummarizer", "granularity" -> "DAY")));

        val table4 = sc.cassandraTable[DeviceUsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).where("device_id=?", "0b303d4d66d13ad0944416780e52cc3db1feba87").first
        table4.avg_num_launches.get should be(0.25)
        table4.avg_time.get should be(2.5)
        table4.start_time.get should be(1460627512768L)
        table4.end_time.get should be(1461669647260L)
        table4.num_days.get should be(12)
        table4.num_contents.get should be(4)
        table4.play_start_time.get should be(1460627674628L)
        table4.last_played_on.get should be(1461669647260L)
        table4.total_play_time.get should be(30)
        table4.num_sessions.get should be(3)
        table4.mean_play_time.get should be(10)
        table4.mean_play_time_interval.get should be(520971.32)
        table4.last_played_content.get should be("numeracy_369")

    }
}