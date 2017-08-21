package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.util.Constants

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.toRDDFunctions

class TestUsageSummaryModel extends SparkSpec(null) {

    "UsageSummaryModel" should "generate ME summary events for (all, per user, per content, per tag, per user & per content, per tag & per user, per tag & per content) dimensions" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.registered_tags");
        }

        val tag1 = RegisteredTag("1375b1d70a66a0f2c22dd1096b98030cb7d9bacb", System.currentTimeMillis(), true)
        val tag2 = RegisteredTag("c6ed6e6849303c77c0182a282ebf318aad28f8d1", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1, tag2)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        val rdd = loadFile[DerivedEvent]("src/test/resources/usage-summary-model/me_summary_test_data.log");
        val out = UsageSummaryModel.execute(rdd, None);
        val events = out.collect()
        
        // All Summary
        val allSum = events.filter { x => "all".equals(x.dimensions.tag.getOrElse("")) && "all".equals(x.dimensions.content_id.getOrElse("")) && "all".equals(x.dimensions.uid.get)}
        allSum.size should be(28)
        
        val event_20160909 = allSum.filter { x => 20160920 == x.dimensions.period.get }.apply(0)
        
        event_20160909.eid should be("ME_USAGE_SUMMARY")
        val event_20160909EksMap = event_20160909.edata.eks.asInstanceOf[Map[String, AnyRef]]
        event_20160909EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(147.65)
        event_20160909EksMap.get("total_sessions").get.asInstanceOf[Long] should be(496L)
        event_20160909EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(16.87)
        event_20160909EksMap.get("total_interactions").get.asInstanceOf[Long] should be(20588L)
        event_20160909EksMap.get("total_ts").get.asInstanceOf[Double] should be(73234.46)
        event_20160909EksMap.get("total_users_count").get.asInstanceOf[Long] should be(47L)
        event_20160909EksMap.get("total_content_count").get.asInstanceOf[Long] should be(100L)
        event_20160909EksMap.get("total_devices_count").get.asInstanceOf[Long] should be(42L)
        
        // user_id Summary
        val userSum = events.filter { x => "all".equals(x.dimensions.tag.get) && "all".equals(x.dimensions.content_id.get) && !"all".equals(x.dimensions.uid.get) }
        userSum.size should be(221)
        val userIdSum = userSum.filter { x => "8e758e18-8647-412f-96a0-d3c5b532f285".equals(x.dimensions.uid.get) }
        userIdSum.size should be(3)
        val userIdSum_20160901 = userIdSum.filter { x => 20160901 == x.dimensions.period.get }.apply(0)

        userIdSum_20160901.eid should be("ME_USAGE_SUMMARY")

        val userIdSum_20160901EksMap = userIdSum_20160901.edata.eks.asInstanceOf[Map[String, AnyRef]]

        userIdSum_20160901EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(97.86)
        userIdSum_20160901EksMap.get("total_sessions").get.asInstanceOf[Long] should be(2L)
        userIdSum_20160901EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(81.24)
        userIdSum_20160901EksMap.get("total_interactions").get.asInstanceOf[Long] should be(265)
        userIdSum_20160901EksMap.get("total_ts").get.asInstanceOf[Double] should be(195.72)
        userIdSum_20160901EksMap.get("total_users_count").get.asInstanceOf[Long] should be(0)
        userIdSum_20160901EksMap.get("total_content_count").get.asInstanceOf[Long] should be(2L)
        userIdSum_20160901EksMap.get("total_devices_count").get.asInstanceOf[Long] should be(1L)
        
        // Content Summary
        val contentSum = events.filter { x => "all".equals(x.dimensions.tag.get) && !"all".equals(x.dimensions.content_id.get) && "all".equals(x.dimensions.uid.get) }
        contentSum.size should be(555)

        val do_30031115Sum = contentSum.filter { x => "do_30031115".equals(x.dimensions.content_id.get) }
        do_30031115Sum.size should be(13)
        val do_30031115Sum_20160901 = do_30031115Sum.filter { x => 20160901 == x.dimensions.period.get }.apply(0)
               
        do_30031115Sum_20160901.eid should be("ME_USAGE_SUMMARY")

        val do_30031115Sum_20160901EksMap = do_30031115Sum_20160901.edata.eks.asInstanceOf[Map[String, AnyRef]]

        do_30031115Sum_20160901EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(1.48)
        do_30031115Sum_20160901EksMap.get("total_sessions").get.asInstanceOf[Long] should be(1L)
        do_30031115Sum_20160901EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(81.08)
        do_30031115Sum_20160901EksMap.get("total_interactions").get.asInstanceOf[Long] should be(2L)
        do_30031115Sum_20160901EksMap.get("total_ts").get.asInstanceOf[Double] should be(1.48)
        do_30031115Sum_20160901EksMap.get("total_users_count").get.asInstanceOf[Long] should be(1L)
        do_30031115Sum_20160901EksMap.get("total_devices_count").get.asInstanceOf[Long] should be(1L)
        do_30031115Sum_20160901EksMap.get("total_content_count").get.asInstanceOf[Long] should be(0L)

        // Tag Summary
        val tagSum = events.filter { x => !"all".equals(x.dimensions.tag.get) && "all".equals(x.dimensions.content_id.get) && "all".equals(x.dimensions.uid.get) }
        tagSum.size should be(8)

        val tag1Sum = tagSum.filter { x => "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb".equals(x.dimensions.tag.get) }
        tag1Sum.size should be(2)
        val tag1Sum_20160902 = tag1Sum.filter { x => 20160902 == x.dimensions.period.get }.apply(0)

        tag1Sum_20160902.eid should be("ME_USAGE_SUMMARY")

        val tag1Sum_20160902EksMap = tag1Sum_20160902.edata.eks.asInstanceOf[Map[String, AnyRef]]

        tag1Sum_20160902EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(109)
        tag1Sum_20160902EksMap.get("total_sessions").get.asInstanceOf[Long] should be(1L)
        tag1Sum_20160902EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(3.3)
        tag1Sum_20160902EksMap.get("total_interactions").get.asInstanceOf[Long] should be(6L)
        tag1Sum_20160902EksMap.get("total_ts").get.asInstanceOf[Double] should be(109)
        tag1Sum_20160902EksMap.get("total_users_count").get.asInstanceOf[Long] should be(1L)
        tag1Sum_20160902EksMap.get("total_devices_count").get.asInstanceOf[Long] should be(1L)
        tag1Sum_20160902EksMap.get("total_content_count").get.asInstanceOf[Long] should be(1L)
        

        val tagOther = events.filter { x => "becb887fe82f24c644482eb30041da6d88bd8150".equals(x.dimensions.tag.get) || "fd0a685649d43e543d9ccd22b3b341b42fb1f5c5".equals(x.dimensions.tag.get) }
        tagOther.size should be(0)

        // user_id content_id Summary
        val contentUserSum = events.filter { x => "all".equals(x.dimensions.tag.get) && !"all".equals(x.dimensions.uid.get) && !"all".equals(x.dimensions.content_id.get) }

        contentUserSum.size should be(1026)

        val content1UserSum = contentUserSum.filter { x => "do_30031115".equals(x.dimensions.content_id.get) }
        content1UserSum.size should be(22)
        val contentUser1Sum_20160920 = content1UserSum.filter { x => 20160920 == x.dimensions.period.get }
        contentUser1Sum_20160920.size should be(2)
        
        val content1UserSum_do_30031115 = contentUser1Sum_20160920.filter { x => "d802fb50-322d-4250-92f8-cf6eeaeec37f".equals(x.dimensions.uid.get) }.apply(0)
        
        content1UserSum_do_30031115.eid should be("ME_USAGE_SUMMARY")

        val content1UserSum_do_30031115EksMap = content1UserSum_do_30031115.edata.eks.asInstanceOf[Map[String, AnyRef]]

        content1UserSum_do_30031115EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(26.68)
        content1UserSum_do_30031115EksMap.get("total_sessions").get.asInstanceOf[Long] should be(5)
        content1UserSum_do_30031115EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(18.89)
        content1UserSum_do_30031115EksMap.get("total_interactions").get.asInstanceOf[Long] should be(42)
        content1UserSum_do_30031115EksMap.get("total_ts").get.asInstanceOf[Double] should be(133.38)
        content1UserSum_do_30031115EksMap.get("total_users_count").get.asInstanceOf[Long] should be(0)
        content1UserSum_do_30031115EksMap.get("total_devices_count").get.asInstanceOf[Long] should be(1L)
        content1UserSum_do_30031115EksMap.get("total_content_count").get.asInstanceOf[Long] should be(0L)
        
        // tag content Summary
        val tagContentSum = events.filter { x => !"all".equals(x.dimensions.tag.get) && !"all".equals(x.dimensions.content_id.get) && "all".equals(x.dimensions.uid.get) }

        tagContentSum.size should be(222)

        val tag1ContentSum = tagContentSum.filter { x => "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb".equals(x.dimensions.tag.get) }
        tag1ContentSum.size should be(12)
        val tag1Sum_20160920 = tag1ContentSum.filter { x => 20160920 == x.dimensions.period.get }
        tag1Sum_20160920.size should be(11)

        val tag1Sum_do_30031115 = tag1Sum_20160920.filter { x => "do_30031115".equals(x.dimensions.content_id.get) }.last

        tag1Sum_do_30031115.eid should be("ME_USAGE_SUMMARY")

        val tag1Sum_do_30031115EksMap = tag1Sum_do_30031115.edata.eks.asInstanceOf[Map[String, AnyRef]]

        tag1Sum_do_30031115EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(26.68)
        tag1Sum_do_30031115EksMap.get("total_sessions").get.asInstanceOf[Long] should be(5L)
        tag1Sum_do_30031115EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(18.89)
        tag1Sum_do_30031115EksMap.get("total_interactions").get.asInstanceOf[Long] should be(42L)
        tag1Sum_do_30031115EksMap.get("total_ts").get.asInstanceOf[Double] should be(133.38)
        tag1Sum_do_30031115EksMap.get("total_devices_count").get.asInstanceOf[Long] should be(1L)
        tag1Sum_do_30031115EksMap.get("total_content_count").get.asInstanceOf[Long] should be(0L)
        
        // tag user_id Summary
        val tagUserSum = events.filter { x => !"all".equals(x.dimensions.tag.get) && !"all".equals(x.dimensions.uid.get) && "all".equals(x.dimensions.content_id.get) }

        tagUserSum.size should be(25)

        val tag1UserSum = tagUserSum.filter { x => "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb".equals(x.dimensions.tag.get) }
        tag1UserSum.size should be(3)
        val tagUser1Sum_20160920 = tag1UserSum.filter { x => 20160920 == x.dimensions.period.get }
        tagUser1Sum_20160920.size should be(2)
        
        val tag1UserSum_do_30031115 = tagUser1Sum_20160920.filter { x => "d802fb50-322d-4250-92f8-cf6eeaeec37f".equals(x.dimensions.uid.get) }.apply(0)
        
        tag1UserSum_do_30031115.eid should be("ME_USAGE_SUMMARY")

        val tag1UserSum_do_30031115EksMap = tag1UserSum_do_30031115.edata.eks.asInstanceOf[Map[String, AnyRef]]

        tag1UserSum_do_30031115EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(62.07)
        tag1UserSum_do_30031115EksMap.get("total_sessions").get.asInstanceOf[Long] should be(17)
        tag1UserSum_do_30031115EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(26.21)
        tag1UserSum_do_30031115EksMap.get("total_interactions").get.asInstanceOf[Long] should be(461)
        tag1UserSum_do_30031115EksMap.get("total_ts").get.asInstanceOf[Double] should be(1055.14)
        tag1UserSum_do_30031115EksMap.get("total_users_count").get.asInstanceOf[Long] should be(0) 
        tag1UserSum_do_30031115EksMap.get("total_devices_count").get.asInstanceOf[Long] should be(1L)
        tag1UserSum_do_30031115EksMap.get("total_content_count").get.asInstanceOf[Long] should be(9L)
    }

    it should "test the summary for one week ss data as input" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.registered_tags");
        }

        val tag1 = RegisteredTag("42d3b7edc2e9b59a286b1956e3cdbc492706ac21", System.currentTimeMillis(), true)
        val tag2 = RegisteredTag("c6ed6e6849303c77c0182a282ebf318aad28f8d1", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1, tag2)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        val rdd = loadFile[DerivedEvent]("src/test/resources/usage-summary-model/test_data_one_week.log");
        val out = UsageSummaryModel.execute(rdd, None);
        val events = out.collect()

        // All Summary
        val allSum = events.filter { x => "all".equals(x.dimensions.tag.getOrElse("")) && "all".equals(x.dimensions.content_id.getOrElse("")) && "all".equals(x.dimensions.uid.getOrElse("")) }
        allSum.size should be(5)
        val event_20160916 = allSum.filter { x => 20160916 == x.dimensions.period.get }.last
        event_20160916.eid should be("ME_USAGE_SUMMARY")
        val event_20160916EksMap = event_20160916.edata.eks.asInstanceOf[Map[String, AnyRef]]
        event_20160916EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(103.4)
        event_20160916EksMap.get("total_sessions").get.asInstanceOf[Long] should be(203)
        event_20160916EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(22.6)
        event_20160916EksMap.get("total_interactions").get.asInstanceOf[Long] should be(7907L)
        event_20160916EksMap.get("total_ts").get.asInstanceOf[Double] should be(20990.23)

        // Tag Summary
        val tagSum = events.filter { x => !"all".equals(x.dimensions.tag.get) && "all".equals(x.dimensions.content_id.get) && "all".equals(x.dimensions.uid.get) }
        tagSum.size should be(5)

        val tag1Sum = tagSum.filter { x => "42d3b7edc2e9b59a286b1956e3cdbc492706ac21".equals(x.dimensions.tag.get) }
        tag1Sum.size should be(3)
        val tag1Sum_20160912 = tag1Sum.filter { x => 20160912 == x.dimensions.period.get }.last

        tag1Sum_20160912.eid should be("ME_USAGE_SUMMARY")

        val tag1Sum_20160912EksMap = tag1Sum_20160912.edata.eks.asInstanceOf[Map[String, AnyRef]]

        tag1Sum_20160912EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(544.13)
        tag1Sum_20160912EksMap.get("total_sessions").get.asInstanceOf[Long] should be(5L)
        tag1Sum_20160912EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(5.93)
        tag1Sum_20160912EksMap.get("total_interactions").get.asInstanceOf[Long] should be(269L)
        tag1Sum_20160912EksMap.get("total_ts").get.asInstanceOf[Double] should be(2720.65)

        val tagOther = events.filter { x => "becb887fe82f24c644482eb30041da6d88bd8150".equals(x.dimensions.tag.get) || "fd0a685649d43e543d9ccd22b3b341b42fb1f5c5".equals(x.dimensions.tag.get) || "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb".equals(x.dimensions.tag.get) }
        tagOther.size should be(0)

    }

    it should "test the summarizer where 1 week of data is missing in the input" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.registered_tags");
        }

        val tag1 = RegisteredTag("1375b1d70a66a0f2c22dd1096b98030cb7d9bacb", System.currentTimeMillis(), true)
        val tag2 = RegisteredTag("c6ed6e6849303c77c0182a282ebf318aad28f8d1", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1, tag2)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        val rdd = loadFile[DerivedEvent]("src/test/resources/usage-summary-model/test_data_1month.log");
        val out = UsageSummaryModel.execute(rdd, None);
        val events = out.collect()
        val lastWeekEvents = events.filter { x => x.dimensions.period.get > 20160924 }
        lastWeekEvents.size should be(0)

        val exceptLastWeek = events.filter { x => x.dimensions.period.get <= 20160924 }
        exceptLastWeek.size should be(events.size)
        events.size should be > (0)
    }
}