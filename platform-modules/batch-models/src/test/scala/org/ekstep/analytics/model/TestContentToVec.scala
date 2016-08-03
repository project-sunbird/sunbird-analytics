package org.ekstep.analytics.model

class TestContentToVec extends SparkSpec(null) {
    
    ignore should "update content_to_vec tabel and generates enriched json" in {
            val jsonRdd = ContentToVec.execute(null, Option(Map("content_limit" -> Int.box(10))));
            val jsons = jsonRdd.collect
            jsons.size should be (1)
            println(jsons(0))
    }
}