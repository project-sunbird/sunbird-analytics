package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.JSONUtils

case class RegisteredTagTest(tag_id: String, last_updated: Long, active: Boolean)

class TestContentUsageSummaryModel extends SparkSpec(null) {
    
    "ContentUsageSummaryModel" should "generate content summary events for (all, per content, per tag, per tag & per content) dimensions" in {

        val tag1 = RegisteredTagTest("1375b1d70a66a0f2c22dd1096b98030cb7d9bacb", System.currentTimeMillis(), true)
        val tag2 = RegisteredTagTest("c6ed6e6849303c77c0182a282ebf318aad28f8d1", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1, tag2)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        
        val rdd = loadFile[DerivedEvent]("src/test/resources/content-usage-summary-model/content_summary_test_data.log");
        val out = ContentUsageSummaryModel.execute(rdd, None);
        val events = out.collect()
        
        // All Summary
        val allSum = events.filter { x => "all".equals(x.dimensions.tag.getOrElse("")) && "all".equals(x.dimensions.content_id.getOrElse("")) }
        allSum.size should be(27)
        val event_20160909 = allSum.filter { x => 20160909==x.dimensions.period.get }.last
        event_20160909.eid should be("ME_CONTENT_USAGE_SUMMARY")
        event_20160909.mid should be("AD5F4DA4D69C8145165873FAD5F9F6CA")
        val event_20160909EksMap = event_20160909.edata.eks.asInstanceOf[Map[String, AnyRef]]
        event_20160909EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(273.25)
        event_20160909EksMap.get("total_sessions").get.asInstanceOf[Long] should be(8L)
        event_20160909EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(13.26)
        event_20160909EksMap.get("total_interactions").get.asInstanceOf[Long] should be(483L)
        event_20160909EksMap.get("total_ts").get.asInstanceOf[Double] should be(2186.01)

        // Content Summary
        val contentSum = events.filter { x => "all".equals(x.dimensions.tag.get) && !"all".equals(x.dimensions.content_id.get) }
        contentSum.size should be (551)
        
        val do_30031115Sum = contentSum.filter { x => "do_30031115".equals(x.dimensions.content_id.get) }
        do_30031115Sum.size should be (13)
        val do_30031115Sum_20160901 = do_30031115Sum.filter { x => 20160901==x.dimensions.period.get }.last
        
        do_30031115Sum_20160901.eid should be("ME_CONTENT_USAGE_SUMMARY")
        do_30031115Sum_20160901.mid should be("4E5A472417612E50AE9F42BA81D9C30F")
        
        val do_30031115Sum_20160901EksMap = do_30031115Sum_20160901.edata.eks.asInstanceOf[Map[String, AnyRef]]
        
        do_30031115Sum_20160901EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(1.48)
        do_30031115Sum_20160901EksMap.get("total_sessions").get.asInstanceOf[Long] should be(1L)
        do_30031115Sum_20160901EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(81.08)
        do_30031115Sum_20160901EksMap.get("total_interactions").get.asInstanceOf[Long] should be(2L)
        do_30031115Sum_20160901EksMap.get("total_ts").get.asInstanceOf[Double] should be(1.48)
        
        // Tag Summary
        val tagSum = events.filter { x => !"all".equals(x.dimensions.tag.get) && "all".equals(x.dimensions.content_id.get) }
        tagSum.size should be (8)
        
        val tag1Sum = tagSum.filter { x => "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb".equals(x.dimensions.tag.get) }
        tag1Sum.size should be (2)
        val tag1Sum_20160902 = tag1Sum.filter { x => 20160902==x.dimensions.period.get }.last
        
        tag1Sum_20160902.eid should be("ME_CONTENT_USAGE_SUMMARY")
        tag1Sum_20160902.mid should be("AE5FB7B9A2DD14D83F72576E894E01FB")
        
        val tag1Sum_20160902EksMap = tag1Sum_20160902.edata.eks.asInstanceOf[Map[String, AnyRef]]
        
        tag1Sum_20160902EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(109)
        tag1Sum_20160902EksMap.get("total_sessions").get.asInstanceOf[Long] should be(1L)
        tag1Sum_20160902EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(3.3)
        tag1Sum_20160902EksMap.get("total_interactions").get.asInstanceOf[Long] should be(6L)
        tag1Sum_20160902EksMap.get("total_ts").get.asInstanceOf[Double] should be(109)
        
        val tagOther = events.filter { x => "becb887fe82f24c644482eb30041da6d88bd8150".equals(x.dimensions.tag.get) || "fd0a685649d43e543d9ccd22b3b341b42fb1f5c5".equals(x.dimensions.tag.get) }
        tagOther.size should be(0)
        
        
        // tag Contenent Summary
        val tagContentSum = events.filter { x => !"all".equals(x.dimensions.tag.get) && !"all".equals(x.dimensions.content_id.get) }

        tagContentSum.size should be (219)
        
        val tag1ContentSum = tagContentSum.filter { x => "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb".equals(x.dimensions.tag.get) }
        tag1ContentSum.size should be (12)
        val tag1Sum_20160920 = tag1ContentSum.filter { x => 20160920==x.dimensions.period.get }
        tag1Sum_20160920.size should be (11)
        
        val tag1Sum_do_30031115 = tag1Sum_20160920.filter { x => "do_30031115".equals(x.dimensions.content_id.get) }.last
        

        tag1Sum_do_30031115.eid should be("ME_CONTENT_USAGE_SUMMARY")
        tag1Sum_do_30031115.mid should be("3FCD946E5DC86992B052A46E70911C94")
        
        val tag1Sum_do_30031115EksMap = tag1Sum_do_30031115.edata.eks.asInstanceOf[Map[String, AnyRef]]
        
        tag1Sum_do_30031115EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(26.68)
        tag1Sum_do_30031115EksMap.get("total_sessions").get.asInstanceOf[Long] should be(5L)
        tag1Sum_do_30031115EksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(18.89)
        tag1Sum_do_30031115EksMap.get("total_interactions").get.asInstanceOf[Long] should be(42L)
        tag1Sum_do_30031115EksMap.get("total_ts").get.asInstanceOf[Double] should be(133.38)
            
        //becb887fe82f24c644482eb30041da6d88bd8150 , c6ed6e6849303c77c0182a282ebf318aad28f8d1, 1375b1d70a66a0f2c22dd1096b98030cb7d9bacb, fd0a685649d43e543d9ccd22b3b341b42fb1f5c5

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM content_db.registered_tags WHERE tag_id='1375b1d70a66a0f2c22dd1096b98030cb7d9bacb'");
            session.execute("DELETE FROM content_db.registered_tags WHERE tag_id='c6ed6e6849303c77c0182a282ebf318aad28f8d1'");
        }
    }
}