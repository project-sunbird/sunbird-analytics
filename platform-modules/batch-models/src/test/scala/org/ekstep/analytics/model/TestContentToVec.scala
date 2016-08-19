package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil

class TestContentToVec extends SparkSpec(null) {

    it should "update content_to_vec tabel and generates enriched json for 10 contents" in {

        val jobParams = Map(
            "content2vec.s3_bucket" -> "sandbox-data-store",
            "content2vec.s3_key_prefix" -> "model/",
            "content2vec.model_path" -> "/tmp/content2vec/model",
            "content2vec.kafka_topic" -> "sandbox.learning.graph.events",
            "content2vec.kafka_broker_list" -> "localhost:9092",
            "content2vec.corpus_path" -> "/tmp/content2vec/content_corpus",
            "python.home" -> "/usr/local/bin/",
            "content2vec.download_path" -> "/tmp/content2vec/download",
            "content2vec.search_request" -> Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 5)))
        val jsonRdd = ContentVectorsModel.execute(null, Option(jobParams));
        CommonUtil.deleteDirectory("/tmp/content2vec")
        jsonRdd.collect.length should be(5)
        //println("Total vectors produced", jsonRdd.count);
    }
}