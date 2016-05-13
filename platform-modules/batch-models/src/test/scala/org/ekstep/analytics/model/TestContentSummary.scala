package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

class TestContentSummary extends SparkSpec(null) {
    
    "ContentSummary" should "generate contentsummary and pass all positive test cases" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("truncate content_db.contentcumulativesummary");
        }
        
        val cs = ContentSummary("org.ekstep.vayuthewind", DateTime.now(), 0L, 0.0, 0.0, 0L, 0.0, 0L, 0.0,"","")
        val crdd = sc.parallelize(Array(cs));
        crdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE);
        
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content-summary/test_data_1.log");
        val me = ContentActivitySummary.execute(rdd, None).collect();

        me.length should be(1);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        
        event1.eid should be("ME_CONTENT_SUMMARY");
        event1.mid should be("1080007CF2C413B606497B29D3AD5909");
        event1.context.pdata.model should be("ContentSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;

        val eks = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks.get("averageTimeSpent").get should be(70.93)
        eks.get("averageInteractionsPerMin").get should be(22.27)
        eks.get("timeSpent").get should be(0.06)
        eks.get("totalInteractionEvents").get should be(79)
        eks.get("tsPerWeek").get should be(212.8)
        eks.get("totalSessions").get should be(3)
        eks.get("sessionsPerWeek").get should be(3)
        eks.get("contentType").get should be("Story")

        val rdd2 = loadFile[MeasuredEvent]("src/test/resources/content-summary/test_data_2.log");
        val me2 = ContentActivitySummary.execute(rdd2, None).collect();
        val event2 = JSONUtils.deserialize[MeasuredEvent](me2(0));
        
        val eks2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks2.get("averageTimeSpent").get should be(102.66)
        eks2.get("averageInteractionsPerMin").get should be(28.64)
        eks2.get("timeSpent").get should be(0.14)
        eks2.get("totalInteractionEvents").get should be(245)
        eks2.get("tsPerWeek").get should be(513.28)
        eks2.get("totalSessions").get should be(5)
        eks2.get("sessionsPerWeek").get should be(5)
        eks2.get("contentType").get should be("Story")
        
        val rdd3 = loadFile[MeasuredEvent]("src/test/resources/content-summary/test_data_3.log");
        val me3 = ContentActivitySummary.execute(rdd3, None).collect();
        val event3 = JSONUtils.deserialize[MeasuredEvent](me3(0));
        
        val eks3 = event3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks3.get("averageTimeSpent").get should be(110.97)
        eks3.get("averageInteractionsPerMin").get should be(24.6)
        eks3.get("timeSpent").get should be(0.18)
        eks3.get("totalInteractionEvents").get should be(273)
        eks3.get("tsPerWeek").get should be(332.91)
        eks3.get("totalSessions").get should be(6)
        eks3.get("sessionsPerWeek").get should be(3)
        eks3.get("contentType").get should be("Story")
    }
    
    it should "generate contentsummary for more than 5 content, those are not Collection type" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content-summary/test_data_4.log");
        val me = ContentActivitySummary.execute(rdd, None).collect;
    }
}