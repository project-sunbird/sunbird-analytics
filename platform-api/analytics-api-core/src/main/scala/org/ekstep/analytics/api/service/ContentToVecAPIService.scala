package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.Response
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.util.JSONUtils

object ContentToVecAPIService {
    
    val BASE_URL = "http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/content/"
    
    def getEnrichedJson(contentId: String)(implicit sc: SparkContext): String = {
        
        val contentArr = Array(BASE_URL+contentId)
        val enrichedJson = sc.makeRDD(contentArr).pipe("python /Users/amitBehera/github/ekstep/Learning-Platform-Analytics/platform-scripts/python/main/vidyavaani/content/enrich_content.py").collect
        JSONUtils.serialize(enrichedJson.last);
        
    }
    
    def updateVec() {
        
    }
}
