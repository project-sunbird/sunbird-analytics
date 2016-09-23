package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants

class TestContentSideloadingSummary extends SparkSpec(null) {

    "ContentSideloadingSummary" should "generate content sideloading summary events" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("truncate content_db.content_sideloading_summary");
        }

        val rdd = loadFile[Event]("src/test/resources/content-sideloading-summary/test_data_1.log");
        val rdd2 = ContentSideloadingSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.length should be(2)
        val event1 = events(0);

        event1.syncts should be(1459849146717l)
        event1.eid should be("ME_CONTENT_SIDELOADING_SUMMARY");
        event1.mid should be("56C7D87F4E861BD50E97076168607FBD");
        event1.context.pdata.model should be("ContentSideloadingSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("CUMULATIVE");
        event1.context.date_range should not be null;
        event1.content_id.get should be("org.ekstep.story.en.family")

        val eks = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks.get("num_downloads").get should be(2)
        eks.get("num_sideloads").get should be(3)
    }

    it should "generate 1 content sideloading summary event and pass all positive test cases" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("truncate content_db.content_sideloading_summary");
        }

        val rdd1 = loadFile[Event]("src/test/resources/content-sideloading-summary/test_data_2.log");
        val rdd2 = ContentSideloadingSummaryModel.execute(rdd1, None);
        val events1 = rdd2.collect
        events1.length should be(1)

        val event1 = events1(0);

        event1.eid should be("ME_CONTENT_SIDELOADING_SUMMARY");
        event1.mid should be("56C7D87F4E861BD50E97076168607FBD");
        event1.context.pdata.model should be("ContentSideloadingSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("CUMULATIVE");
        event1.context.date_range should not be null;
        event1.content_id.get should be("org.ekstep.story.en.family")

        val eks1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks1.get("num_downloads").get should be(1)
        eks1.get("num_sideloads").get should be(3)
        eks1.get("avg_depth").get should be(3.5)

        val rdd3 = loadFile[Event]("src/test/resources/content-sideloading-summary/test_data_3.log");
        val rdd4 = ContentSideloadingSummaryModel.execute(rdd3, None);
        val events2 = rdd4.collect
        events2.length should be(1)

        val event2 = events2(0);

        event2.eid should be("ME_CONTENT_SIDELOADING_SUMMARY");
        event2.mid should be("56C7D87F4E861BD50E97076168607FBD");
        event2.context.pdata.model should be("ContentSideloadingSummary");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("CUMULATIVE");
        event2.context.date_range should not be null;
        event2.content_id.get should be("org.ekstep.story.en.family")

        val eks2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks2.get("num_downloads").get should be(1)
        eks2.get("num_sideloads").get should be(5)
        eks2.get("avg_depth").get should be(4)

        val table1 = sc.cassandraTable[ContentSideloading](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY).where("content_id=?", "org.ekstep.story.en.family").first
        table1.origin_map.size should be(2)

        val rdd5 = loadFile[Event]("src/test/resources/content-sideloading-summary/test_data_4.log");
        val rdd6 = ContentSideloadingSummaryModel.execute(rdd5, None);
        val events3 = rdd6.collect

        val table2 = sc.cassandraTable[ContentSideloading](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY).where("content_id=?", "org.ekstep.story.en.family").first
        table2.origin_map.size should be(3)

        val event3 = events3(0);

        val eks3 = event3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks3.get("num_downloads").get should be(1)
        eks3.get("num_sideloads").get should be(7)
        eks3.get("avg_depth").get should be(3)
    }
}