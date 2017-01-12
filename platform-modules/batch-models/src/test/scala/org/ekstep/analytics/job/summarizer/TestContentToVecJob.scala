package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestContentToVecJob extends SparkSpec(null) {
  
    it should "populate to ContentToVec DB" in {
        val jobParams = Map(
            "content2vec.s3_bucket" -> "ekstep-dev-data-store",
            "content2vec.s3_key_prefix" -> "model/",
            "content2vec.model_path" -> "/tmp/content2vec/model",
            "content2vec.kafka_topic" -> "sandbox.learning.graph.events",
            "content2vec.kafka_broker_list" -> "localhost:9092",
            "content2vec.corpus_path" -> "/tmp/content2vec/content_corpus",
            //"python.home" -> "/usr/local/bin/",
            "content2vec_scripts_path" -> "src/test/resources/python/main/vidyavaani",
            "content2vec.download_path" -> "/tmp/content2vec/download",
            "content2vec.search_request" -> Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 5)))
            
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.ContentVectorsModel", Option(jobParams), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestContentToVecJob"), Option(false))
        ContentToVecJob.main(JSONUtils.serialize(config))(Option(sc));
    }   
}