package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestContentUsageSummary extends SparkSpec(null) {
    
    it should "generate content summary events" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content_usage_summary/*");
        val rdd2 = ContentUsageSummary.execute(rdd, None);
        val events = rdd2.collect
        for(e<-events){
            println(e)
        }
        events.length should be (29)
        val summ = JSONUtils.deserialize[MeasuredEvent](events(0))
        summ.content_id.get should be ("org.ekstep.story.ka.elephant")
        summ.dimensions.group_user.get should be (false)
        
        val eksMap = summ.edata.eks.asInstanceOf[Map[String,AnyRef]]
        eksMap.get("total_ts").get should be (196.8)
        eksMap.get("total_sessions").get should be (8)
        eksMap.get("avg_ts_session").get should be (24.6)
        eksMap.get("total_interactions").get should be (89)
        eksMap.get("avg_interactions_min").get should be (27.13)
        eksMap.get("content_type").get should be ("Story")
        eksMap.get("mime_type").get should be ("application/vnd.ekstep.ecml-archive")
    }
}