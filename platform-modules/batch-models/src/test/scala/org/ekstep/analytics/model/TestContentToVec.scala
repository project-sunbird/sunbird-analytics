package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils

class TestContentToVec extends SparkSpec(null) {
    
    "ContentToVec" should "update content_to_vec tabel and generates enriched json" in {
        val req = """{"request":{"filters":{"objectType":["Content"],"contentType":["Story","Worksheet","Collection","Game"],"status":["Live"]},"limit":5}}"""
        val content_search_request = JSONUtils.deserialize[Map[String, AnyRef]](req)
        val jsonRdd = ContentToVec.execute(null, Option(Map("content_search_request" -> content_search_request)));
            val jsons = jsonRdd.collect
            jsons.size should be (5)
            println(JSONUtils.serialize(jsons(0)))
    }
}