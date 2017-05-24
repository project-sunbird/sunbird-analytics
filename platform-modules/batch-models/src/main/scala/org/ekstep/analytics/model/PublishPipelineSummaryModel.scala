package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.AlgoOutput
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.joda.time.format.DateTimeFormat
import java.util.Formatter.DateTime
import java.util.Formatter.DateTime
import java.util.Date
import java.text.SimpleDateFormat
import org.ekstep.analytics.creation.model.CreationEvent

case class EventsByPeriod(period: Long, events: Buffer[CreationEvent]) extends AlgoInput
case class ContentPublishSummary(draft_count: Long, review_count: Long, live_count: Long)
case class ItemPublishSummary(created_count: Long)
case class AssetPublishSummary(audio_created_count: Long, video_created_count: Long, image_created_count: Long, plugin_created_count: Long)
case class TextbookPublishSummary(created_count: Long, live_count: Long)
case class PipelineSummaryOutput(
  content_publish: ContentPublishSummary,
  item_publish: ItemPublishSummary,
  asset_publish: AssetPublishSummary,
  textbook_publish: TextbookPublishSummary,
  date_range: DtRange,
  period: Long) extends AlgoOutput

object PublishPipelineSummaryModel extends IBatchModelTemplate[CreationEvent, EventsByPeriod, PipelineSummaryOutput, MeasuredEvent] with Serializable {

  implicit val className = "org.ekstep.analytics.model.PublishPipelineSummaryModel"

  override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EventsByPeriod] = {
    JobLogger.log("Filtering Events of BE_OBJECT_LIFECYCLE")
    val objectLifecycleEvents = DataFilter.filter(data, Array(Filter("eventId", "IN", Option(List("BE_OBJECT_LIFECYCLE")))));
    objectLifecycleEvents.map { x =>
      val d = new Date(x.ets)
      val period = CommonUtil.getPeriod(x.ets, Period.DAY)
      (period, Buffer(x))
    }.partitionBy(new HashPartitioner(JobContext.parallelization))
      .reduceByKey((a, b) => a ++ b)
      .map(x => EventsByPeriod(x._1, x._2))
  }

  override def algorithm(input: RDD[EventsByPeriod], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PipelineSummaryOutput] = {

    val data = input.cache()
    val summaries = data.map { d =>
      val draftsCount = d.events.count(x => x.edata.eks.`type` == "Content" && x.edata.eks.state == "Draft")
      val reviewsCount = d.events.count(x => x.edata.eks.`type` == "Content" && x.edata.eks.state == "Review")
      val liveCount = d.events.count(x => x.edata.eks.`type` == "Content" && x.edata.eks.state == "Live")
      val createdCount = d.events.count(x => x.edata.eks.`type` == "Item" && x.edata.eks.state == "Live")
      val audioCreatedCount = d.events.count(x => x.edata.eks.`type` == "Asset" && x.edata.eks.state == "Live" && x.edata.eks.subtype == "audio")
      val videoCreatedCount = d.events.count(x => x.edata.eks.`type` == "Asset" && x.edata.eks.state == "Live" && x.edata.eks.subtype == "video")
      val imageCreatedCount = d.events.count(x => x.edata.eks.`type` == "Asset" && x.edata.eks.state == "Live" && x.edata.eks.subtype == "image")
      val pluginCreatedCount = d.events.count(x => x.edata.eks.`type` == "Plugin" && x.edata.eks.state == "Live")
      val textbookCreatedCount = d.events.count(x => x.edata.eks.`type` == "Textbook" && x.edata.eks.state == "Draft")
      val textbookLiveCount = d.events.count(x => x.edata.eks.`type` == "Textbook" && x.edata.eks.state == "Live")

      val dateRange = DtRange(d.events.head.ets, d.events.last.ets)
      PipelineSummaryOutput(ContentPublishSummary(draftsCount, reviewsCount, liveCount),
        ItemPublishSummary(createdCount),
        AssetPublishSummary(audioCreatedCount, videoCreatedCount, imageCreatedCount, pluginCreatedCount),
        TextbookPublishSummary(textbookCreatedCount, textbookLiveCount),
        dateRange, d.period)
    }

    data.unpersist(true)

    summaries
  }

  override def postProcess(data: RDD[PipelineSummaryOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
    data.map { x =>
      val measures = Map(
        "content_publish" -> x.content_publish,
        "item_publish" -> x.item_publish,
        "asset_publish" -> x.asset_publish,
        "textbook_publish" -> x.textbook_publish)
      val mid = CommonUtil.getMessageId("ME_PUBLISH_PIPELINE_SUMMARY", x.period.toString(), "DAY", x.date_range, "");
      MeasuredEvent("ME_PUBLISH_PIPELINE_SUMMARY", System.currentTimeMillis(), x.date_range.to, "1.0", mid, "", None, None,
        Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "PublishPipelineSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", x.date_range),
        Dimensions(None, None, None, None, None, None, None, None, None, None, Option(x.period.toInt)),
        MEEdata(measures), None);
    }
  }
}
