package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.conf.AppConf

class TestContentVectorsModel extends SparkSpec(null) {

    ignore should "update content_to_vec tabel and generates enriched json for 10 contents" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.content_to_vector");
        }

        val jobParams = Map(
            "content2vec.s3_bucket" -> "ekstep-dev-data-store",
            "content2vec.s3_key_prefix" -> "model/",
            "content2vec.model_path" -> "/tmp/content2vec/model",
            "content2vec.kafka_topic" -> "sandbox.learning.graph.events",
            "content2vec.kafka_broker_list" -> "localhost:9092",
            "content2vec.corpus_path" -> "/tmp/content2vec/content_corpus",
            //"python.home" -> "/usr/local/bin/",
            "content2vec.download_path" -> "/tmp/content2vec/download",
            "content2vec_scripts_path" -> "src/test/resources/python/main/vidyavaani",
            "content2vec.search_request" -> Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 1)))

        val runTime = Runtime.getRuntime
        val scriptPath = jobParams.get("content2vec_scripts_path").get.asInstanceOf[String]

        val procGetCon = runTime.exec("python " + scriptPath + "/content/get_concepts.py");
        procGetCon.waitFor()
        procGetCon.exitValue() should be(0)
        procGetCon.destroy()

        val procEnrich = runTime.exec("python " + scriptPath + "/content/enrich_content.py");
        procEnrich.waitFor()
        procEnrich.exitValue() should be(0)
        procEnrich.destroy()

        val json = """{"languageCode": "en", "mediaCount": {"gif": 0, "ogg": 0, "png": 12, "mp3": 1, "jpg": 0}, "contentType": "Worksheet", "description": "PRW Item Template", "language": ["English"], "items": ["{}"], "imageTags": ["icon validate", "greenbox", "icon home", "icon submit", "ekstep placeholder blue eye", "icon hint", "speech bubble", "yellowbox", "icon reload", "background", "icon next", "icon previous"], "mp3Transcription": {"/Users/amitBehera/contentToVecTest/tmp/domain_38527/assets/q1_1460133785285": {"alternative": [{"confidence": 0.86655968, "transcript": "Ek gajar Ki Keemat kya hai"}, {"transcript": "gajar Ki Keemat kya hai"}, {"transcript": "IIT gajar Ki Keemat kya hai"}, {"transcript": "Ik gajar Ki Keemat kya hai"}, {"transcript": "EC gajar Ki Keemat kya hai"}], "final": true}}, "mp3Length": 2.914104308390023, "content_corpusepts": [], "owner": "EkStep", "identifier": "domain_38527", "data": ["{}"], "developer": "EkStep"}"""
        val contentToCorpus = sc.makeRDD(Array(json)).pipe("python " + scriptPath + "/object2vec/update_content_corpus.py").collect;
        contentToCorpus.size should be > (0)

        val procTrain = runTime.exec("python " + scriptPath + "/object2vec/corpus_to_vec.py");
        procTrain.waitFor()
        procTrain.exitValue() should be(0)
        procTrain.destroy()

        val procInfer = runTime.exec("python " + scriptPath + "/object2vec/infer_query.py");
        procInfer.waitFor()
        procInfer.exitValue() should be(0)
        procInfer.destroy()

        val jsonRdd = ContentVectorsModel.execute(null, Option(jobParams));
        CommonUtil.deleteDirectory("/tmp/content2vec")

        val contentToVecTable = sc.cassandraTable[ContentVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC).collect
        contentToVecTable.size should not be (0)
    }
}