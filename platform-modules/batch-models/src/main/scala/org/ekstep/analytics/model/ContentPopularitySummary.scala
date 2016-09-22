package org.ekstep.analytics.model

import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.GData
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.ListBuffer
import org.joda.time.DateTime
import org.ekstep.analytics.framework.ContentKey

case class ContentPopularitySummary(ck: ContentKey, comments: Array[String], rating: Array[Double], avg_rating: Double, downloads: Int, side_loads: Int, dt_range: DtRange, syncts: Long, gdata: Option[GData] = None) extends AlgoOutput;
case class InputEventsContentPopularity(ck: ContentKey, events: Buffer[ContentPopularitySummary]) extends Input with AlgoInput

object ContentPopularitySummary extends IBatchModelTemplate[Event, InputEventsContentPopularity, ContentPopularitySummary, MeasuredEvent] with Serializable {

	val className = "org.ekstep.analytics.model.ContentPopularitySummary"
	override def name: String = "ContentPopularitySummarizer"
	
	private def _computeMetrics(events: Buffer[ContentPopularitySummary], ck: ContentKey): ContentPopularitySummary = {
		val firstEvent = events.sortBy { x => x.dt_range.from }.head;
        val lastEvent = events.sortBy { x => x.dt_range.from }.last;
        val ck = firstEvent.ck;
        
        val gdata = if (StringUtils.equals(ck.content_id, "all")) None else Option(new GData(ck.content_id, firstEvent.gdata.get.ver));
        val dt_range = DtRange(firstEvent.dt_range.from, lastEvent.dt_range.from);
        val downloads = events.map { x => x.downloads }.sum;
        val side_loads = events.map { x => x.side_loads }.sum;
        val comments = events.map { x => x.comments }.flatMap { x => x }.filter { x => !StringUtils.isEmpty(x) }.toArray;
        val rating = events.map { x => x.rating }.flatMap { x => x }.filter { x => x != 0.0 }.toArray;
        val avg_rating = if (rating.length > 0) CommonUtil.roundDouble(rating.sum/rating.length, 2) else 0.0;
        ContentPopularitySummary(ck, comments, rating, avg_rating, downloads, side_loads, dt_range, lastEvent.syncts, gdata);
	}
	
	private def getContentPopularitySummary(event: Event, period: Int, contentId: String, tagId: String): Array[ContentPopularitySummary] = {
		val dt_range = DtRange(event.ets, event.ets);
		if ("GE_FEEDBACK".equals(event.eid)) {
			val ck = ContentKey(period, contentId, tagId);
			val gdata = event.gdata;
			val comments = event.edata.eks.comments;
			val rating = event.edata.eks.rating;
			val avg_rating = rating;
			Array(ContentPopularitySummary(ck, Array(comments), Array(rating), avg_rating, 0, 0, dt_range, CommonUtil.getEventSyncTS(event), Option(gdata)));
		} else if ("GE_TRANSFER".equals(event.eid)) {
			val contents = event.edata.eks.contents;
			contents.map { content => 
				val tsContentId = if ("all".equals(contentId)) contentId else content.get("identifier").get.asInstanceOf[String];
				val ck = ContentKey(period, tsContentId, tagId);
				val gdata = if ("all".equals(contentId)) None else Option(new GData(tsContentId, content.get("pkgVersion").getOrElse("").toString));
				val transferCount = content.get("transferCount").get.asInstanceOf[Double];
				val downloads = if (transferCount == 0.0) 1 else 0;
				val side_loads = if (transferCount >= 1.0) 1 else 0;
				ContentPopularitySummary(ck, Array(), Array(), 0.0, downloads, side_loads, dt_range, CommonUtil.getEventSyncTS(event), gdata);
			}
		} else {
			Array();
		}
	}
	
	override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[InputEventsContentPopularity] = {
		val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).select("tag_id").where("active = ?", true).map { x => x.tag_id }.collect
        val registeredTags = if (tags.nonEmpty) tags; else Array[String]();
		
		val transferEvents = DataFilter.filter(data, Array(Filter("uid", "ISNOTEMPTY", None), Filter("eid", "EQ", Option("GE_TRANSFER"))));
		val importEvents = DataFilter.filter(DataFilter.filter(transferEvents, Filter("edata.eks.direction", "EQ", Option("IMPORT"))), Filter("edata.eks.datatype", "EQ", Option("CONTENT")));
		val feedbackEvents = DataFilter.filter(data, Array(Filter("uid", "ISNOTEMPTY", None), Filter("eid", "EQ", Option("GE_FEEDBACK"))));
		val normalizeEvents = importEvents.union(feedbackEvents).map { event => 
			var list: ListBuffer[ContentPopularitySummary] = ListBuffer[ContentPopularitySummary]();
            val period = CommonUtil.getPeriod(event.ets, Period.DAY);
            
            list ++= getContentPopularitySummary(event, period, "all", "all");
            list ++= getContentPopularitySummary(event, period, event.gdata.id, "all");
            val tags = _getValidTags(Option(event.tags), registeredTags);
            for (tag <- tags) {
            	list ++= getContentPopularitySummary(event, period, "all", tag); // for tag
                list ++= getContentPopularitySummary(event, period, event.gdata.id, tag); // for tag and content
            }
            list.toArray
		}.flatMap { x => x };
		
		normalizeEvents.map { x => (x.ck, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => InputEventsContentPopularity(x._1, x._2) };
	}

	override def algorithm(data: RDD[InputEventsContentPopularity], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentPopularitySummary] = {
		data.map { x =>
            _computeMetrics(x.events, x.ck);
        }
	}

	override def postProcess(data: RDD[ContentPopularitySummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
		data.map { cpMetrics =>  
			val mid = CommonUtil.getMessageId("ME_CONTENT_POPULARITY_SUMMARY", cpMetrics.ck.content_id + cpMetrics.ck.tag + cpMetrics.ck.period, "DAY", cpMetrics.syncts);
			val measures = Map(
                "downloads" -> cpMetrics.downloads,
                "side_loads" -> cpMetrics.side_loads,
                "avg_rating" -> cpMetrics.avg_rating,
                "rating" -> cpMetrics.rating,
                "comments" -> cpMetrics.comments)
			
            MeasuredEvent("ME_CONTENT_POPULARITY_SUMMARY", System.currentTimeMillis(), cpMetrics.dt_range.to, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentPopularitySummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], cpMetrics.dt_range),
                Dimensions(None, None, cpMetrics.gdata, None, None, None, None, None, None, Option(cpMetrics.ck.tag), Option(cpMetrics.ck.period), Option(cpMetrics.ck.content_id)),
                MEEdata(measures));
		}
	}
	
	private def _getValidTags(tags: Option[AnyRef], registeredTags: Array[String]): Array[String] = {
        val tagList = tags.getOrElse(List()).asInstanceOf[List[Map[String, List[String]]]]
        val genieTagFilter = if (tagList.nonEmpty) tagList.filter(f => f.contains("genie")) else List()
        val tempList = if (genieTagFilter.nonEmpty) genieTagFilter.filter(f => f.contains("genie")).last.get("genie").get; else List();
        tempList.filter { x => registeredTags.contains(x) }.toArray;
    }
}