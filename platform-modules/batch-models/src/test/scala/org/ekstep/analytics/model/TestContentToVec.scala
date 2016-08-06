package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils

class TestContentToVec extends SparkSpec(null) {

    ignore should "update content_to_vec tabel and generates enriched json" in {

        val jobParams = Map(
            "content2vec.content_service_url" -> "https://dev.ekstep.in/api/learning",
            "content2vec.scripts_path" -> "../../platform-scripts/python/main/vidyavaani",
            "content2vec.s3_bucket" -> "sandbox-data-store",
            "content2vec.s3_key_prefix" -> "model/",
            "content2vec.model_path" -> "content2vec/",
            "content2vec.kafka_topic" -> "sandbox.learning.graph.events",
            "content2vec.kafka_broker_list" -> "localhost:9092",
            "content2vec.corpus_path" -> "content_corpus",
            "python.home" -> "/usr/local/bin/",
            "content2vec.download_path" -> "/Users/santhosh/ekStep/content2vec/tmp/temp",
            "content2vec.search_request" -> Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 1000)))
        val jsonRdd = ContentVectorsModel.execute(null, Option(jobParams));
        println("Total vectors produced", jsonRdd.count);
    }
}