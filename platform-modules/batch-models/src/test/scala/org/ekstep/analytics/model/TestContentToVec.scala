package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils

class TestContentToVec extends SparkSpec(null) {
    
    "ContentToVec" should "update content_to_vec tabel and generates enriched json" in {
            val jsonRdd = ContentToVec.execute(null, Option(Map("content_limit" -> Int.box(10))));
            val jsons = jsonRdd.collect
            //jsons.size should be (10)
            println(JSONUtils.serialize(jsons(0)))
    }
}