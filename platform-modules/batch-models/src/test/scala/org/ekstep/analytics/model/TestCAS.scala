package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.CommonUtil

class TestCAS extends SparkSpec(null) {
  
    "ContentSummary" should "generate contentsummary and pass all positive test cases" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("truncate content_db.content_cumulative_summary");
        }
        
        val cs = ContentSumm("org.ekstep.vayuthewind", DateTime.now(), 0L, 0.0, 0.0, 0L, 0.0, 0L, 0.0,"","")
        val crdd = sc.parallelize(Array(cs));
        crdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE);
        
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content-summary/test_data_1.log");
        val me = CAS.execute(rdd, None).collect();

        me.length should be(1);
        
        val event1 = CommonUtil.getMeasuredEvent(me(0))
        
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
    }
}