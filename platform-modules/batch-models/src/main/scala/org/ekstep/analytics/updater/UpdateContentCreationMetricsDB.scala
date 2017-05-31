/**
 * @author Mahesh Kumar Gangula
 */

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
import org.ekstep.analytics.framework.util.CommonUtil

case class ContentElementsCount(content_id: String, plugins: List[String], assets: List[String]) extends AlgoInput
case class ContentCreationMetrics(d_content_id: String, d_ver: Int, tags_count: Int, images_count: Int, audios_count: Int, videos_count: Int, plugin_metrics: Map[String, Int], time_spent_draft: Option[Double], time_spent_review: Option[Double], last_status: Option[String], last_status_date: Option[Long], pkg_version: Int, updated_date: Long, first_ver_total_sessions: Long = 0L, first_ver_total_ts: Double = 0.0) extends AlgoOutput with Output
case class ContentCreationMetricsIndex(d_content_id: String);
case class StatusChange(status: String, ets: Long);

/**
 * @dataproduct
 * @updater 
 * 
 * UpdateContentCreationMetricsDB
 * 
 * Functionality
 * 1. Updater to update the creation metrics of each content.
 * 		a. Computation of elements (media, plugins) count.
 * 		b. Time spent in draft and reveiw status.
 * 		c. Total time spent and session to create first version content.
 * 
 * Dependencies:
 * 	1. BE_OBJECT_LIFE_CYCLE events	
 * 	2. UpdateContentEditorUsageDB data product
 */

object UpdateContentCreationMetricsDB extends IBatchModelTemplate[CreationEvent, CreationEvent, ContentCreationMetrics, ContentCreationMetrics] with Serializable {

	override def name(): String = "UpdateContentCreationMetricsDB";
	implicit val className = "org.ekstep.analytics.updater.UpdateContentCreationMetricsDB";

	val TIME_SPENT_STATUS_LIST = List("Draft", "Reveiw");

	private def _getGraphMetrics(query: String, key1: String, key2: String)(implicit sc: SparkContext): Map[String, Int] = {
		GraphQueryDispatcher.dispatch(query).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x => (x.get(key1).asString(), x.get(key2).asInt()) }.toMap
	}
	override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationEvent] = {
		JobLogger.log("Filtering Events of BE_OBJECT_LIFECYCLE")
		DataFilter.filter(data, Array(Filter("eventId", "IN", Option(List("BE_OBJECT_LIFECYCLE")))))
			.filter { x => "Content".equals(x.edata.eks.`type`) }.cache();
	}

	override def algorithm(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentCreationMetrics] = {

		val currentData = getContentElementMetrics();
		val existingData = currentData.map { x => x._1 }.joinWithCassandraTable[ContentCreationMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE).on(SomeColumns("d_content_id"))
		val joinedRDD = currentData.leftOuterJoin(existingData)
		val elementsCountRDD = joinedRDD.map { f =>
			val current = f._2._1;
			val existing = f._2._2.getOrElse(f._2._1);
			(f._1, ContentCreationMetrics(current.d_content_id, current.d_ver, current.tags_count, current.images_count, current.audios_count, current.videos_count, current.plugin_metrics, existing.time_spent_draft, existing.time_spent_review, existing.last_status, existing.last_status_date, current.pkg_version, current.updated_date));
		};
		
		val liveContentMetrics = data.filter { x => "Live".equals(x.edata.eks.state) }
			.map { x => x.edata.eks.id }.distinct().map { x => CEUsageSummaryIndex(0, x) }
			.joinWithCassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).on(SomeColumns("d_period", "d_content_id"))
			.map(f => (f._1.d_content_id, f._2)).collectAsMap().toMap

		val lifecycleEvents = data.map { x => (ContentCreationMetricsIndex(x.edata.eks.id), Buffer(x)) }
			.partitionBy(new HashPartitioner(JobContext.parallelization))
			.reduceByKey((a, b) => a ++ b).map { f => (f._1, f._2.sortBy { x => x.ets }) };

		val lifecycleJoinedRDD = elementsCountRDD.leftOuterJoin(lifecycleEvents);
		lifecycleJoinedRDD.map { f =>
			val withoutLC = f._2._1;
			if (f._2._2.isEmpty) 
				withoutLC 
			else {
				val lcEvents = f._2._2.get;
				// Update time spent for draft and review status.
				val withLC = updateLifecycleMetrics(withoutLC, lcEvents);
				// Check for first version created to update sessions and time spent.
				val liveCount = lcEvents.count { x => "Live".equals(x.edata.eks.state) };
				if (withoutLC.pkg_version == liveCount) updateCreationMetrics(withLC, liveContentMetrics) else withLC;
			}
		}
	}

	/**
	 * Get all contents with elements (image, video, audio, plugins) count.
	 */
	private def getContentElementMetrics()(implicit sc: SparkContext): RDD[(ContentCreationMetricsIndex, ContentCreationMetrics)] = {
		val contentData = sc.cassandraTable[ContentData](Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE)
			.map { x => (x.content_id, new String(x.body.getOrElse(Array()), "UTF-8")) }.filter { x => !x._2.isEmpty }
			.map { x => parseECMLContent(x._1, x._2) }.filter { x => null != x }

		val contentTagCountMap = _getGraphMetrics(CypherQueries.PER_CONTENT_TAGS, "contentId", "tagCount")
		val contentLivesCountMap = _getGraphMetrics(CypherQueries.CONTENT_LIVE_COUNT, "contentId", "liveCount")
		val updatedAt = System.currentTimeMillis();

		contentData.map { x =>
			val assetMetrics = x.assets.groupBy { x => x }.map { x => (x._1, x._2.length) }
			val pluginMetrics = x.plugins.groupBy { x => x }.map { x => (x._1, x._2.length) } - ("appEvents", "events", "#PCDATA", "manifest", "config", "param")
			val content = x.content_id
			val tags = contentTagCountMap.getOrElse(content, 0)
			val liveCount = contentLivesCountMap.getOrElse(content, 0)
			val timeSpentDraft = 0.0;
			val timeSpentReview = 0.0;
			(ContentCreationMetricsIndex(x.content_id), ContentCreationMetrics(x.content_id, 0, tags, assetMetrics.getOrElse("image", 0), assetMetrics.getOrElse("sound", 0) + assetMetrics.getOrElse("audiosprite", 0), assetMetrics.getOrElse("video", 0), pluginMetrics, Option(timeSpentDraft), Option(timeSpentReview), None, None, liveCount, updatedAt))
		};
	}

	/**
	 * Compute time spent in each status.
	 */
	private def updateLifecycleMetrics(metric: ContentCreationMetrics, lifeCycleEvents: Buffer[CreationEvent]): ContentCreationMetrics = {
		val currentLCChanges = lifeCycleEvents.map { x => StatusChange(x.edata.eks.state, x.ets) }.sortBy { x => x.ets }
		val lifeCycleChanges = if (metric.last_status.isEmpty) currentLCChanges else Buffer(StatusChange(metric.last_status.get, metric.last_status_date.get)) ++ currentLCChanges;
		if (lifeCycleChanges(0).ets > metric.last_status_date.getOrElse(0L)) {
			var tmpLastChange: StatusChange = null;
			val statusWithTs = lifeCycleChanges.map { x =>
				if (tmpLastChange == null) tmpLastChange = x;
				val ts = CommonUtil.getTimeDiff(tmpLastChange.ets, x.ets).get;
				val status = tmpLastChange.status;
				tmpLastChange = x;
				(status, ts);
			}.groupBy(f => f._1).map(f => (f._1, f._2.map(f => f._2).sum));
			val tsDraft = CommonUtil.roundDouble(metric.time_spent_draft.getOrElse(0.0) + (statusWithTs.getOrElse("Draft", 0.0) / 60), 2)
			val tsReview = CommonUtil.roundDouble(metric.time_spent_draft.getOrElse(0.0) + (statusWithTs.getOrElse("Review", 0.0) / 60), 2)
			val lastLCChange = lifeCycleChanges.last;
			ContentCreationMetrics(metric.d_content_id, metric.d_ver, metric.tags_count, metric.images_count, metric.audios_count, metric.videos_count, metric.plugin_metrics, Option(tsDraft), Option(tsReview), Option(lastLCChange.status), Option(lastLCChange.ets), metric.pkg_version, metric.updated_date);
		} else {
			metric;
		}
	}

	private def updateCreationMetrics(metric: ContentCreationMetrics, liveContentMetrics: Map[String, CEUsageSummaryFact]): ContentCreationMetrics = {
		val contentId = metric.d_content_id;
		val summary = liveContentMetrics.getOrElse(contentId, CEUsageSummaryFact(0, contentId, 0L, 0L, 0.0, 0.0, 0L));
		ContentCreationMetrics(metric.d_content_id, metric.d_ver, metric.tags_count, metric.images_count, metric.audios_count, metric.videos_count, metric.plugin_metrics, metric.time_spent_draft, metric.time_spent_review, metric.last_status, metric.last_status_date, metric.pkg_version, metric.updated_date, summary.total_sessions, summary.total_ts);
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