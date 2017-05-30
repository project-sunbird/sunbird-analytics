package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.AlgoOutput
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.AlgoInput
import scala.xml.XML
import scala.collection.mutable.Buffer
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.util.ContentData
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.util.CypherQueries
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext

case class ContentElementsCount(content_id: String, plugins: List[String], assets: List[String]) extends AlgoInput
case class ContentCreationMetrics(d_content_id: String, d_ver: Int, tags_count: Int, images_count: Int, audios_count: Int, videos_count: Int, plugin_metrics: Map[String, Int], time_spent_draft: Option[Double], time_spent_review: Option[Double], current_status: Option[String], status_updated_date: Option[Long], pkg_version: Int, updated_date: Long) extends AlgoOutput with Output
case class ContentCreationMetricsIndex(d_content_id: String);
case class ContentCreationMetricsInput(index: ContentCreationMetricsIndex, filteredEvents: Buffer[CreationEvent]) extends AlgoInput;

object UpdateContentCreationMetricsDB extends IBatchModelTemplate[CreationEvent, ContentCreationMetricsInput, ContentCreationMetrics, ContentCreationMetrics] with Serializable {

	override def name(): String = "UpdateContentCreationMetricsDB";
	implicit val className = "org.ekstep.analytics.updater.UpdateContentCreationMetricsDB";
	
	val TIME_SPENT_STATUS_LIST = List("Draft", "Reveiw");

	private def _getGraphMetrics(query: String, key1: String, key2: String)(implicit sc: SparkContext): Map[String, Int] = {
		GraphQueryDispatcher.dispatch(query).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x => (x.get(key1).asString(), x.get(key2).asInt()) }.toMap
	}
	override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentCreationMetricsInput] = {
		JobLogger.log("Filtering Events of BE_OBJECT_LIFECYCLE")
		val lifecycleEvents = DataFilter.filter(data, Array(Filter("eventId", "IN", Option(List("BE_OBJECT_LIFECYCLE")))));
		lifecycleEvents.filter { x => "Content".equals(x.edata.eks.`type`) }
		.map { x => (ContentCreationMetricsIndex(x.edata.eks.id), Buffer(x)) }
		.partitionBy(new HashPartitioner(JobContext.parallelization))
        .reduceByKey((a, b) => a ++ b).map { f => ContentCreationMetricsInput(f._1, f._2.sortBy { x => x.ets })};
	}

	override def algorithm(data: RDD[ContentCreationMetricsInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentCreationMetrics] = {
		
		val contentData = sc.cassandraTable[ContentData](Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE)
			.map { x => (x.content_id, new String(x.body.getOrElse(Array()), "UTF-8")) }.filter { x => !x._2.isEmpty }
			.map { x => parseECMLContent(x._1, x._2) }.filter { x => null != x }

		val contentTagCountMap = _getGraphMetrics(CypherQueries.PER_CONTENT_TAGS, "contentId", "tagCount")
		val contentLivesCountMap = _getGraphMetrics(CypherQueries.CONTENT_LIVE_COUNT, "contentId", "liveCount")
		val updatedAt = System.currentTimeMillis();

		val currentData = contentData.map { x =>
			val assetMetrics = x.assets.groupBy { x => x }.map { x => (x._1, x._2.length) }
			val pluginMetrics = x.plugins.groupBy { x => x }.map { x => (x._1, x._2.length) } - ("appEvents", "events", "#PCDATA", "manifest", "config", "param")
			val content = x.content_id
			val tags = contentTagCountMap.getOrElse(content, 0)
			val liveCount = contentLivesCountMap.getOrElse(content, 0)
			val timeSpentDraft = 0.0;
			val timeSpentReview = 0.0;
			(ContentCreationMetricsIndex(x.content_id), ContentCreationMetrics(x.content_id, 0, tags, assetMetrics.getOrElse("image", 0), assetMetrics.getOrElse("sound", 0) + assetMetrics.getOrElse("audiosprite", 0), assetMetrics.getOrElse("video", 0), pluginMetrics, Option(timeSpentDraft), Option(timeSpentReview), None, None, liveCount, updatedAt))
		};

		val existingData = currentData.map { x => x._1 }.joinWithCassandraTable[ContentCreationMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE).on(SomeColumns("d_content_id"))
		val joinedRDD = currentData.leftOuterJoin(existingData)
		val elementsCountRDD = joinedRDD.map { f =>
			val current = f._2._1;
			val existing = f._2._2.getOrElse(f._2._1);
			(f._1, ContentCreationMetrics(current.d_content_id, current.d_ver, current.tags_count, current.images_count, current.audios_count, current.videos_count, current.plugin_metrics, existing.time_spent_draft, existing.time_spent_review, existing.current_status, existing.status_updated_date, current.pkg_version, current.updated_date));
		};
		
		val lifecycleJoinedRDD = elementsCountRDD.leftOuterJoin(data.map { x => (x.index, x.filteredEvents) });
		lifecycleJoinedRDD.map { f =>
			val withoutLC = f._2._1;
			val withLC = if(f._2._2.isEmpty) withoutLC else updateLifecycleMetrics(withoutLC, f._2._2.get);
			if(withoutLC.pkg_version == 0 && withLC.pkg_version > 0) updateCreationMetrics(withLC) else withLC;
		}
	}
	
	private def updateLifecycleMetrics(metric: ContentCreationMetrics, lifeCycleEvents: Buffer[CreationEvent]): ContentCreationMetrics = {
		// TODO: compute and update life cycle metrics.
		metric;
	}
	
	private def updateCreationMetrics(metric: ContentCreationMetrics): ContentCreationMetrics = {
		// TODO: compute and update life cycle metrics.
		metric;
	}
	
	
	override def postProcess(data: RDD[ContentCreationMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentCreationMetrics] = {
		data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE);
		data;
	}

	private def parseECMLContent(contentId: String, body: String): ContentElementsCount = {
		try {
			val dom = XML.loadString(body)
			val els = dom \ "manifest" \ "media"
			val stage = dom \ "stage"
			val plugins = stage.map { x => x.child }.flatMap { x => x }.map { x => x.head.label }.toList
			val assests = els.map { x => x.attribute("type").get.text }.toList
			ContentElementsCount(contentId, plugins, assests)
		} catch {
			case t: Throwable =>
				println("Unable to parse ecml content for contentId:" + contentId);
				null;
		}
	}
}