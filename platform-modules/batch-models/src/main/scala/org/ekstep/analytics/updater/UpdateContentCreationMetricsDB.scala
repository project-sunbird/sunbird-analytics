package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.AlgoOutput
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.AlgoInput
import scala.xml.XML
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.util.ContentData
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.util.CypherQueries

case class ContentPluginAsset(content_id: String, plugins: List[String], assets: List[String]) extends AlgoInput
case class ContentCreationMetrics(d_content_id: String, tags: Int, images: Int, audios: Int, videos: Int, plugin_metrics: Map[String, Int], live_times: Int) extends AlgoOutput

object UpdateContentCreationMetricsDB extends IBatchModelTemplate[Empty, ContentPluginAsset, ContentCreationMetrics, Empty] with Serializable {

    override def name(): String = "UpdateContentCreationMetricsDB";
    implicit val className = "org.ekstep.analytics.updater.UpdateContentCreationMetricsDB";

    private def _getGraphMetrics(query: String, key1: String, key2: String)(implicit sc: SparkContext): Map[String, Int] = {
        GraphQueryDispatcher.dispatch(query).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x => (x.get(key1).asString(), x.get(key2).asInt()) }.toMap
    }
    override def preProcess(input: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentPluginAsset] = {
        val contentData = sc.cassandraTable[ContentData](Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE)
            .map { x => (x.content_id, new String(x.body.getOrElse(Array()), "UTF-8")) }.filter { x => !x._2.isEmpty }
            .map { x => parseECMLContent(x._1, x._2) }.filter { x => null != x }
        contentData
    }

    override def algorithm(data: RDD[ContentPluginAsset], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentCreationMetrics] = {

        val contentTagCountMap = _getGraphMetrics(CypherQueries.PER_CONTENT_TAGS, "contentId", "tagCount")
        val contentLivesCountMap = _getGraphMetrics(CypherQueries.CONTENT_LIVE_COUNT, "contentId", "liveCount")

        data.map { x =>
            val assetMetrics = x.assets.groupBy { x => x }.map { x => (x._1, x._2.length) }
            val pluginMetrics = x.plugins.groupBy { x => x }.map { x => (x._1, x._2.length) }

            val content = x.content_id
            val tags = contentTagCountMap.getOrElse(content, 0)
            val liveCount = contentLivesCountMap.getOrElse(content, 0)
            ContentCreationMetrics(x.content_id, tags, assetMetrics.getOrElse("image", 0), assetMetrics.getOrElse("sound", 0) + assetMetrics.getOrElse("audiosprite", 0), assetMetrics.getOrElse("video", 0), pluginMetrics, liveCount)
        }
    }
    override def postProcess(data: RDD[ContentCreationMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE);
        sc.makeRDD(Seq(Empty()));
    }

    private def parseECMLContent(contentId: String, body: String): ContentPluginAsset = {
        try {
            val dom = XML.loadString(body)
            val els = dom \ "manifest" \ "media"
            val stage = dom \ "stage"
            val plugins = stage.map { x => x.child }.flatMap { x => x }.map { x => x.head.label }.filter { x =>
                (!StringUtils.equals("#PCDATA", x) && !StringUtils.equals("manifest", x) && !StringUtils.equals("config", x) && !StringUtils.equals("param", x))
            }.toList
            val assests = els.map { x => x.attribute("type").get.text }.toList
            ContentPluginAsset(contentId, plugins, assests)
        } catch {
            case t: Throwable =>
                t.printStackTrace()
                println("Unable to parse ecml content for contentId:" + contentId);
                null;
        }
    }
}