package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.RestUtil
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.util.S3Util
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Empty

case class Params(resmsgid: String, msgid: String, err: String, status: String, errmsg: String);
case class Response(id: String, ver: String, ts: String, params: Params, result: Option[Map[String, AnyRef]]);
case class ContentVectors(content_vectors: Array[ContentVector]);
class ContentVector(val contentId: String,val text_vec: List[Double], val tag_vec: List[Double]);
case class ContentURL(content_url: String, base_url: String) extends AlgoInput
case class ContentEnrichedJson(contentId: String, jsonData: Map[String, AnyRef]) extends AlgoOutput


object ContentToVec extends IBatchModelTemplate[Empty, ContentURL, ContentEnrichedJson, MeasuredEvent] with Serializable {
  
    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentURL] = {
        
        val contentUrl = AppConf.getConfig("content2vec.content_service_url");
        val baseUrl = AppConf.getConfig("service.search.url");
        val searchUrl = s"$baseUrl/v2/search";
        println(searchUrl)
        val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 1));
        val resp = RestUtil.post[Response](searchUrl, JSONUtils.serialize(request));
        val contentList = resp.result.getOrElse(Map("content" -> List())).getOrElse("content", List()).asInstanceOf[List[Map[String, AnyRef]]];
        val contents = contentList.map(f => f.get("identifier").get.asInstanceOf[String]).map { x => s"$contentUrl/v2/content/$x" }
        //sc.parallelize(contents);
        sc.parallelize(contents, 1).map { x => ContentURL(x, contentUrl) };
    }
    
    override def algorithm(data: RDD[ContentURL], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentEnrichedJson] = {
        
         println(data.collect.size+"   all content url size ")
         printRDD(data.map{x => JSONUtils.serialize(x)})
         val scriptLoc = AppConf.getConfig("content2vec.scripts_path");
         val pythonExec = AppConf.getConfig("python.home") + "python";
         val env = Map("PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"));
         println("Python home:"+ pythonExec)
         println("Runncing _doContentEnrichment.......")
         val enrichedContentRDD = _doContentEnrichment(data.map { x => JSONUtils.serialize(x) }, scriptLoc, pythonExec, env).cache();
         printRDD(enrichedContentRDD)
         val corpusRDD = _doContentToCorpus(enrichedContentRDD, scriptLoc, pythonExec);
         println("Running _doContentToCorpus........")

         _doTrainContent2VecModel(scriptLoc, pythonExec);
         val vectors = _doUpdateContentVectors(corpusRDD, scriptLoc, pythonExec, null);

         enrichedContentRDD.map {  x => 
             val jsonData = JSONUtils.deserialize[Map[String, AnyRef]](x)
             ContentEnrichedJson(jsonData.get("identifier").get.asInstanceOf[String], jsonData);
         };
    }
    
    override def postProcess(data: RDD[ContentEnrichedJson], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { x => getME(x) };
    }
    
    private def _doContentEnrichment(contentRDD: RDD[String], scriptLoc: String, pythonExec: String, env: Map[String, String]): RDD[String] = {

        if (StringUtils.equalsIgnoreCase("true", AppConf.getConfig("content2vec.enrich_content"))) {
            contentRDD.pipe(s"$pythonExec $scriptLoc/content/enrich_content.py", env)
        } else {
            contentRDD
        }
    }

    private def _doContentToCorpus(contentRDD: RDD[String], scriptLoc: String, pythonExec: String): RDD[String] = {

        if (StringUtils.equalsIgnoreCase("true", AppConf.getConfig("content2vec.content_corpus"))) {
            contentRDD.pipe(s"$pythonExec $scriptLoc/object2vec/update_content_corpus.py");
        } else {
            contentRDD
        }
    }

    private def printRDD(rdd: RDD[String]) = {
        println(rdd.collect().last);
    }
    
    private def _doTrainContent2VecModel(scriptLoc: String, pythonExec: String) = {

        if (StringUtils.equalsIgnoreCase("true", AppConf.getConfig("content2vec.train_model"))) {
            val bucket = AppConf.getConfig("content2vec.s3_bucket");
            val modelPath = AppConf.getConfig("content2vec.model_path");
            val prefix = AppConf.getConfig("content2vec.s3_key_prefix");
            ScriptDispatcher.dispatch(Array(), Map("script" -> s"$pythonExec $scriptLoc/object2vec/corpus_to_vec.py",
                "corpus_loc" -> AppConf.getConfig("content2vec.corpus_path"), "model" -> modelPath))
            S3Util.uploadDirectory(bucket, prefix, modelPath);
        }
    }

    private def _doUpdateContentVectors(contentRDD: RDD[String], scriptLoc: String, pythonExec: String, contentId: String): RDD[String] = {

        val bucket = AppConf.getConfig("content2vec.s3_bucket");
        val modelPath = AppConf.getConfig("content2vec.model_path");
        val prefix = AppConf.getConfig("content2vec.s3_key_prefix");
        S3Util.download(bucket, prefix, modelPath)
        val vectorRDD = contentRDD.map { x =>
            Map("contentId" -> contentId, "document" -> JSONUtils.deserialize[Map[String, AnyRef]](x), "infer_all" -> AppConf.getConfig("content2vec.infer_all"),
                "corpus_loc" -> AppConf.getConfig("content2vec.corpus_path"), "model" -> modelPath);
        }.map(JSONUtils.serialize).pipe(s"$pythonExec $scriptLoc/object2vec/infer_query.py");

        val x = vectorRDD.map { x => JSONUtils.deserialize[ContentVectors](x) }.flatMap { x => x.content_vectors.map { y => y } }.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC)
        vectorRDD;
    }
    
    private def getME(data: ContentEnrichedJson): MeasuredEvent = {
        val ts = System.currentTimeMillis()
        val dateRange = DtRange(ts, ts)
        val mid = org.ekstep.analytics.framework.util.CommonUtil.getMessageId("AN_ENRICHED_CONTENT", null, null, dateRange, data.contentId);
        MeasuredEvent("AN_ENRICHED_CONTENT", ts, ts, "1.0", mid, null, Option(data.contentId), None, Context(PData("AnalyticsDataPipeline", "ContentToVec", "1.0"), None, null, dateRange), null, MEEdata(Map("enrichedJson" -> data.jsonData)));
    }
}