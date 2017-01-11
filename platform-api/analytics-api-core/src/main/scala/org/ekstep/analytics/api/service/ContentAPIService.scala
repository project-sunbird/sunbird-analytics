package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Params
import org.ekstep.analytics.api.ContentSummary
import org.ekstep.analytics.api.Trend
import org.ekstep.analytics.api.Range
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.joda.time.Weeks
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.ContentUsageSummaryFact
import org.ekstep.analytics.api.util.CommonUtil
import org.joda.time.DateTimeZone
import org.ekstep.analytics.api.ContentSummary
import java.util.UUID
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.ContentVector
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.DtRange
import sys.process._
import org.ekstep.analytics.framework.util.S3Util
import java.io.File
import org.ekstep.analytics.streaming.KafkaEventProducer
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api.ContentVectors
import akka.actor.Props
import akka.actor.Actor
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.framework.util.RestUtil
import java.net.URL
import org.ekstep.analytics.framework.conf.AppConf
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory


/**
 * @author Santhosh
 */

object ContentAPIService {

    def props = Props[ContentAPIService];
    case class ContentToVec(contentId: String, sc: SparkContext, config: Config);
    case class ContentToVecTrainModel(sc: SparkContext, config: Config);
    case class RecommendationsTrainModel(sc: SparkContext, config: Config);

    def contentToVec(contentId: String)(implicit sc: SparkContext, config: Config): String = {
        val searchBaseUrl = config.getString("service.search.url");
        val defRequest = Map("request" -> Map("filters" -> Map("identifier" -> contentId, "objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 1));
        val request = defRequest;
        val resp = RestUtil.post[Response](s"$searchBaseUrl/v2/search", JSONUtils.serialize(request));
        val contentList = resp.result.getOrElse(Map("content" -> List())).getOrElse("content", List()).asInstanceOf[List[Map[String, AnyRef]]];
        val scriptLoc = config.getString("content2vec.scripts_path");
        val pythonExec = config.getString("python.home") + "python";
        val env = Map("PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"));

        val contentRDD = sc.parallelize(contentList, 1);

        val downloadPath = config.getString("content2vec.download_path");
        val downloadFilePrefix = config.getString("content2vec.download_file_prefix");
        val downloadRDD = contentRDD.map { x => x.getOrElse("downloadUrl", "").asInstanceOf[String] }.filter { x => StringUtils.isNotBlank(x) }.distinct;
        println("Dowload ecars", downloadRDD.collect().mkString);
        downloadRDD.map { downloadUrl =>
            val key = StringUtils.stripStart(org.ekstep.analytics.framework.util.CommonUtil.getPathFromURL(downloadUrl), "/");
            S3Util.downloadFile("ekstep-public", key, downloadPath, downloadFilePrefix);
        }.collect; // Invoke the download

        println("### Ecar file downloaded. Fetching concepts... ###");
        val contentServiceUrl = config.getString("content2vec.content_service_url");
        println("$contentServiceUrl", contentServiceUrl);
        sc.makeRDD(Array(contentServiceUrl), 1).pipe(s"$pythonExec $scriptLoc/content/get_concepts.py").foreach(println);

        println("Calling _doContentEnrichment......")
        val enrichedContentRDD = _doContentEnrichment(contentRDD.map(JSONUtils.serialize), scriptLoc, pythonExec, env).cache();
        printRDD(enrichedContentRDD);
        println("Calling _doContentToCorpus......")
        val corpusRDD = _doContentToCorpus(enrichedContentRDD, scriptLoc, pythonExec, env);

        println("Calling _doTrainContent2VecModel......")
        _doTrainContent2VecModel(scriptLoc, pythonExec, env);
        println("Calling _doUpdateContentVectors......")
        printRDD(corpusRDD);
        val vectors = _doUpdateContentVectors(corpusRDD, scriptLoc, pythonExec, contentId, env);
        org.ekstep.analytics.framework.util.CommonUtil.deleteDirectory(downloadPath);
        vectors.first();
    }

    private def printRDD(rdd: RDD[String]) = {
        rdd.collect().foreach(println);
    }
    
    private def _doContentEnrichment(contentRDD: RDD[String], scriptLoc: String, pythonExec: String, env: Map[String, String])(implicit config: Config): RDD[String] = {

        if (config.getBoolean("content2vec.enrich_content")) {
            contentRDD.pipe(s"$pythonExec $scriptLoc/content/enrich_content.py", env)
        } else {
            contentRDD
        }
    }

    private def _doContentToCorpus(contentRDD: RDD[String], scriptLoc: String, pythonExec: String, env: Map[String, String])(implicit config: Config): RDD[String] = {

        if (config.getBoolean("content2vec.content_corpus")) {
            contentRDD.pipe(s"$pythonExec $scriptLoc/object2vec/update_content_corpus.py", env);
        } else {
            contentRDD
        }
    }

    private def _doTrainContent2VecModel(scriptLoc: String, pythonExec: String, env: Map[String, String])(implicit config: Config) = {

        if (config.getBoolean("content2vec.train_model")) {
            val bucket = config.getString("content2vec.s3_bucket");
            val modelPath = config.getString("content2vec.model_path");
            val prefix = config.getString("content2vec.s3_key_prefix");
            ScriptDispatcher.dispatch(Array[String](), Map("script" -> s"$pythonExec $scriptLoc/object2vec/corpus_to_vec.py",
                "corpus_loc" -> config.getString("content2vec.corpus_path"), "model" -> modelPath))
            S3Util.uploadDirectory(bucket, prefix, modelPath);
        }
    }

    private def _doUpdateContentVectors(contentRDD: RDD[String], scriptLoc: String, pythonExec: String, contentId: String, env: Map[String, String])(implicit config: Config): RDD[String] = {

        val bucket = config.getString("content2vec.s3_bucket");
        val modelPath = config.getString("content2vec.model_path");
        val prefix = config.getString("content2vec.s3_key_prefix");
        S3Util.download(bucket, prefix, modelPath)
        val vectorRDD = contentRDD.map { x =>
            Map("contentId" -> contentId, "document" -> JSONUtils.deserialize[Map[String, AnyRef]](x), "infer_all" -> config.getString("content2vec.infer_all"),
                "corpus_loc" -> config.getString("content2vec.corpus_path"), "model" -> modelPath);
        }.map(JSONUtils.serialize).pipe(s"$pythonExec $scriptLoc/object2vec/infer_query.py");

        val x = vectorRDD.map { x => JSONUtils.deserialize[ContentVectors](x) }.flatMap { x => x.content_vectors.map { y => y } }.saveToCassandra(Constants.CONTENT_DB, Constants.CONTENT_TO_VEC)
        vectorRDD;
    }

    private def _publishEnrichedContent(contentRDD: RDD[String], contentId: String) {
        val enrichedJsonMap = contentRDD.map { x => JSONUtils.deserialize[Map[String, AnyRef]](x) }.collect.last
        val me = JSONUtils.serialize(getME(enrichedJsonMap, contentId))
        //KafkaEventProducer.sendEvents(Array(me), config.get("topic").get, config.get("broker.list").get)
    }

    private def getME(data: Map[String, AnyRef], contentId: String): MeasuredEvent = {
        val ts = System.currentTimeMillis()
        val dateRange = DtRange(ts, ts)
        val mid = org.ekstep.analytics.framework.util.CommonUtil.getMessageId("AN_ENRICHED_CONTENT", null, null, dateRange, contentId);
        MeasuredEvent("AN_ENRICHED_CONTENT", ts, ts, "1.0", mid, null, Option(contentId), None, Context(PData("AnalyticsDataPipeline", "ContentToVec", "1.0"), None, null, dateRange), null, MEEdata(Map("enrichedJson" -> data)));
    }
}

class ContentAPIService extends Actor {
    import ContentAPIService._
    // TODO: update println with logger. **Important**
	// $COVERAGE-OFF$ Disabling scoverage - because actor calls are Async.
    def receive = {
        case ContentToVec(contentId: String, sc: SparkContext, config: Config) =>
            println("ContentToVec (content enrichment) process starting...");
            contentToVec(contentId)(sc, config);
            println("ContentToVec (content enrichment) process completed...");
            sender() ! "success";

        case ContentToVecTrainModel(sc: SparkContext, config: Config) =>
            println("ContentToVec model training starting...");
            val scriptParams = Map(
                "PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"),
                "script" -> config.getString("content2vec.train_model_job"),
                "aws_key" -> AppConf.getAwsKey(),
                "aws_secret" -> AppConf.getAwsSecret());
            ScriptDispatcher.dispatch(Array[String](), scriptParams).foreach(println);
            println("ContentToVec model training completed...");
            sender() ! "success";

        case RecommendationsTrainModel(sc: SparkContext, config: Config) =>
            println("Recommendations model training starting...");
            val scriptParams = Map(
                "PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"),
                "script" -> config.getString("recommendation.train_model_job"),
                "aws_key" -> AppConf.getAwsKey(),
                "aws_secret" -> AppConf.getAwsSecret());
            ScriptDispatcher.dispatch(Array[String](), scriptParams).foreach(println);
            println("Recommendations model training completed...");
            sender() ! "success";
    }
    // $COVERAGE-OFF$
}