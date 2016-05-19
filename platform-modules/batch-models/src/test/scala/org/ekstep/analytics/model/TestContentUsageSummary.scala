package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestContentUsageSummary extends SparkSpec(null) {
    
    it should "generate content summary events" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content_usage_summary/*");
        val rdd2 = ContentUsageSummary.execute(rdd, None);
        val events = rdd2.collect
        events.length should be (29)
        val summ = JSONUtils.deserialize[MeasuredEvent](events(0))
        summ.content_id.get should be ("org.ekstep.esl1")
        summ.dimensions.group_user.get should be (false)
        summ.dimensions.partner_id.get should be ("")
        
        val eksMap = summ.edata.eks.asInstanceOf[Map[String,AnyRef]]
        eksMap.get("total_ts").get should be (3063.81)
        eksMap.get("total_sessions").get should be (40)
        eksMap.get("avg_ts_session").get should be (76.6)
        eksMap.get("total_interactions").get should be (1047)
        eksMap.get("avg_interactions_min").get should be (20.5)
        eksMap.get("content_type").get should be ("Worksheet")
        eksMap.get("mime_type").get should be ("application/vnd.ekstep.ecml-archive")
    }
}