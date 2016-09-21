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


case class ContentKey(period: Int, content_id: String, tag: String);
case class RegisteredTag(tag_id: String)
case class ContentPopularitySummary(ck: ContentKey, comments: Array[String], rating: Array[Double], avg_rating: Double, downloads: Int, side_loads: Int, dt_range: DtRange, syncts: Long, gdata: Option[GData] = None) extends AlgoOutput;
//case class ContentPopularitySummaryInput(eid: String, content_id: String, content_ver: String, ets: Long, syncts: Long, comments: Option[String] = None, rating: Option[Double] = None, transfer_count: Option[Double] = None, tags: Option[AnyRef] = None) extends AlgoInput;
//case class ContentPopularityMetrics(content_id: Option[String] = None, content_ver: Option[String] = None, tag: Option[String] = None, comments: Array[String], rating: Array[Double], avg_rating: Double, downloads: Int, side_loads: Int, dt_range: DtRange) extends AlgoOutput;
//case class CPMInput(day: Int, events: Buffer[ContentPopularitySummaryInput]) extends AlgoInput;
case class InputEvents(ck: ContentKey, events: Buffer[ContentPopularitySummary]) extends Input with AlgoInput

object ContentPopularitySummary extends IBatchModelTemplate[Event, InputEvents, ContentPopularitySummary, MeasuredEvent] with Serializable {

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
		val ck = ContentKey(period, contentId, tagId);
		val dt_range = DtRange(event.ets, event.ets);
		if ("GE_FEEDBACK".equals(event.eid)) {
			val gdata = event.gdata;
			val comments = event.edata.eks.comments;
			val rating = event.edata.eks.rating;
			val avg_rating = rating;
			Array(ContentPopularitySummary(ck, Array(comments), Array(rating), avg_rating, 0, 0, dt_range, CommonUtil.getEventSyncTS(event), Option(gdata)));
		} else if ("GE_TRANSFER".equals(event.eid)) {
			val contents = event.edata.eks.contents;
			contents.map { content => 
				val gdata = new GData(content.get("identifier").get.asInstanceOf[String], content.get("pkgVersion").getOrElse("").toString);
				val transferCount = content.get("transferCount").get.asInstanceOf[Double];
				val downloads = if (transferCount == 0.0) 1 else 0;
				val side_loads = if (transferCount >= 1.0) 1 else 0;
				ContentPopularitySummary(ck, Array(), Array(), 0.0, downloads, side_loads, dt_range, CommonUtil.getEventSyncTS(event), Option(gdata));
			}
		} else {
			Array();
		}
	}
	
	override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[InputEvents] = {
		val tags = sc.cassandraTable[registered_tag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).select("tag_id").where("active = ?", true).map { x => x.tag_id }.collect
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
                list ++= getContentPopularitySummary(event, period, event.gdata.id, tag);
            }
            list.toArray
		}.flatMap { x => x };
		
		normalizeEvents.map { x => (x.ck, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => InputEvents(x._1, x._2) };
		
//		val transferInput = transferEvents.flatMap { x =>
//			val contents = x.edata.eks.contents;
//			contents.map{ f =>
//				val ver = f.get("pkgVersion").getOrElse("").toString;
//				ContentPopularitySummaryInput("GE_TRANSFER", f.get("identifier").get.asInstanceOf[String], "0.0", x.ets, CommonUtil.getEventSyncTS(x), None, None, Option(f.get("transferCount").get.asInstanceOf[Double]), Option(x.tags))	
//			}
//		}
//		
//		val feedbackInput = feedbackEvents.map { x => 
//			ContentPopularitySummaryInput("GE_FEEDBACK", x.gdata.id, x.gdata.ver, x.ets, CommonUtil.getEventSyncTS(x), Option(x.edata.eks.comments), Option(x.edata.eks.rating), None, Option(x.tags));
//		}
//		transferInput.union(feedbackInput).map { event => (CommonUtil.getPeriod(event.ets, Period.DAY), Buffer(event)) }
//			.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b)
//			.map(f => CPMInput(f._1, f._2));
	}

	override def algorithm(data: RDD[InputEvents], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentPopularitySummary] = {
		data.map { x =>
            _computeMetrics(x.events, x.ck);
        }
//		val inputs = data.cache();
//		val allSummary = inputs.map { x =>
//            _getContentPopularityMetrics(x.events);
//        }
//		
//		// per content
//        val contentSummary = inputs.map { x =>
//            x.events.groupBy(f => f.content_id).map { f =>
//                val content_id = f._1
//                val events = f._2
//                _getContentPopularityMetrics(events, Option(content_id));
//            };
//        }.flatMap { x => x }
//        
//        val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).select("tag_id").where("active = ?", true).map { x => x.tag_id }.collect
//        val registeredTags = if (tags.nonEmpty) tags; else Array[String]();
//        
//        // (per tag) & (per tag,per content) summary
//        val tagSummary_tagContentSummary = inputs.map { x =>
//
//            val tagEvents = x.events.map { x =>
//                val genieTagList = _getGenieTagList(x.tags, registeredTags);
//                (genieTagList, Buffer(x));
//            }.map { x => x._1.map { f => (f, x._2) }; }.flatMap(f => f).groupBy(f => f._1)
//
//            // per tag Summary
//            val tagSummary = tagEvents.map { f =>
//                val tag = f._1
//                f._2.map { x => x._2 }.map { x =>
//                    _getContentPopularityMetrics(x, None, Option(tag));
//                }
//            }.flatMap { x => x }
//
//            // per tag per content Summary
//            val tagContentSummary = tagEvents.map { tagEvent =>
//                val tag = tagEvent._1
//                tagEvent._2.map { x => x._2 }.map { x =>
//                    x.groupBy(f => f.content_id).map { f =>
//                        val content_id = f._1
//                        val events = f._2
//                        _getContentPopularityMetrics(events, Option(content_id), Option(tag));
//                    }
//                }.flatMap { x => x };
//            }.flatMap { x => x };
//
//            (tagSummary ++ tagContentSummary);
//
//        }.flatMap { x => x }
//        
//        inputs.unpersist(true)
//        val rdd1 = contentSummary.union(allSummary)
//        rdd1.union(tagSummary_tagContentSummary);
	}

	override def postProcess(data: RDD[ContentPopularitySummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
		data.map { cpMetrics =>  
			val mid = CommonUtil.getMessageId("ME_CONTENT_USAGE_SUMMARY", cpMetrics.ck.content_id + cpMetrics.ck.tag + cpMetrics.ck.period, "DAY", cpMetrics.syncts);
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
	
	private def _getGenieTagList(tags: Option[AnyRef], registeredTags: Array[String]): List[String] = {
		val tagList = tags.getOrElse(List()).asInstanceOf[List[Map[String, List[String]]]]
        val genieTagFilter = if (tagList.nonEmpty) tagList.filter(f => f.contains("genie")) else List()
        val tempList = if (genieTagFilter.nonEmpty) genieTagFilter.filter(f => f.contains("genie")).last.get("genie").get; else List();
        //println(JSONUtils.serialize(tempList))
        tempList.filter { x => registeredTags.contains(x) }
	}
	
	private def _getValidTags(tags: Option[AnyRef], registeredTags: Array[String]): Array[String] = {
        val tagList = tags.getOrElse(List()).asInstanceOf[List[Map[String, List[String]]]]
        val genieTagFilter = if (tagList.nonEmpty) tagList.filter(f => f.contains("genie")) else List()
        val tempList = if (genieTagFilter.nonEmpty) genieTagFilter.filter(f => f.contains("genie")).last.get("genie").get; else List();
        tempList.filter { x => registeredTags.contains(x) }.toArray;
    }
	
//	private def _getContentPopularityMetrics(events: Buffer[ContentPopularitySummaryInput], content_id: Option[String] = None, tag: Option[String] = None): ContentPopularityMetrics = {
//		val sortedEvents = events.sortBy { x => x.ets };
//        val date_range = DtRange(sortedEvents.head.ets, sortedEvents.last.ets);
//        val ver = if (content_id != None) Option(sortedEvents.head.content_ver); else None
//		val feedbackEvents = events.filter { x => "GE_FEEDBACK".equals(x.eid) };
//		val comments = feedbackEvents.map { x => x.comments }.filter {x => !StringUtils.isEmpty(x.get)}.flatten.toArray;
//		val ratings = feedbackEvents.map { x => x.rating }.filter { x => x.isDefined && x.getOrElse(0.0) != 0.0 }.flatten.toArray;
//		val avg_rating = if(ratings.length > 0) CommonUtil.roundDouble((ratings.sum / ratings.length), 2) else 0.0;
//		val transerEvents = events.filter { x => "GE_TRANSFER".equals(x.eid) };
//		val downloads = events.map { x => x.transfer_count }.flatten.toList.count(_ == 0.0);
//		val side_loads = transerEvents.length - downloads;
//		ContentPopularityMetrics(content_id, ver, tag, comments, ratings, avg_rating, downloads, side_loads, date_range);
//	}
}