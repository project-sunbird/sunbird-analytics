package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.DataFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Output
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.model.PipelineSummaryOutput
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.Period
import org.joda.time.format.DateTimeParser
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext

case class PublishPipelineSummaryFact(d_period: Int, audio_created_count: Int, draft_count: Int, image_created_count: Int, item_created_count: Int, live_count: Int, plugin_created_count: Int, review_count: Int, textbook_created_count: Int, textbook_live_count: Int, video_created_count: Int) extends AlgoOutput
case class ContentPublishFactIndex(d_period: Int) extends Output

object UpdatePublishPipelineSummary extends IBatchModelTemplate[DerivedEvent, DerivedEvent, PublishPipelineSummaryFact, ContentPublishFactIndex] with Serializable {
  val className = "org.ekstep.analytics.updater.UpdatePublishPipelineSummary"
  override def name: String = "UpdatePublishPipelineSummary"

  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
    DataFilter.filter(data, Filter("eid", "EQ", Option("ME_PUBLISH_PIPELINE_SUMMARY")));
  }

  override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PublishPipelineSummaryFact] = {
    data.cache()
    computeForPeriod(Period.DAY, data).union(computeForPeriod(Period.WEEK, data)).union(computeForPeriod(Period.MONTH, data)).union(computeForPeriod(Period.CUMULATIVE, data))
  }

  private def computeForPeriod(p: Period, data: RDD[DerivedEvent]): RDD[PublishPipelineSummaryFact] = {
    // combining facts within the current file
    val newData = data.map { d =>
      val s = d.edata.eks.asInstanceOf[Map[String, AnyRef]]
      val ap = s("asset_publish").asInstanceOf[Map[String, Int]]
      val cp = s("content_publish").asInstanceOf[Map[String, Int]]
      val ip = s("item_publish").asInstanceOf[Map[String, Int]]
      val tp = s("textbook_publish").asInstanceOf[Map[String, Int]]
      val d_period = CommonUtil.getPeriod(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(d.dimensions.period.get.toString()), p)
      (ContentPublishFactIndex(d_period), PublishPipelineSummaryFact(d_period, ap("audio_created_count"),
        cp("draft_count"), ap("image_created_count"),
        ip("created_count"), cp("live_count"),
        ap("plugin_created_count"), cp("review_count"),
        tp("created_count"), tp("live_count"), ap("video_created_count")))
    }.reduceByKey(combineFacts)

    val existingData = newData.map { x => x._1 }.joinWithCassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).on(SomeColumns("d_period"))
    val joined = newData.leftOuterJoin(existingData)
    joined.map { d =>
      val newFact = d._2._1
      val existingFact = d._2._2.getOrElse(PublishPipelineSummaryFact(newFact.d_period, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
      combineFacts(newFact, existingFact)
    }
  }

  private def combineFacts(f1: PublishPipelineSummaryFact, f2: PublishPipelineSummaryFact): PublishPipelineSummaryFact = {
    PublishPipelineSummaryFact(f1.d_period, f1.audio_created_count + f2.audio_created_count,
      f1.draft_count + f2.draft_count, f1.image_created_count + f2.image_created_count,
      f1.item_created_count + f2.item_created_count, f1.live_count + f2.live_count,
      f1.plugin_created_count + f2.plugin_created_count, f1.review_count + f2.review_count,
      f1.textbook_created_count + f2.textbook_created_count, f1.textbook_live_count + f2.textbook_live_count,
      f1.video_created_count + f2.video_created_count)
  }

  override def postProcess(data: RDD[PublishPipelineSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentPublishFactIndex] = {
    data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT)
    data.map { d => ContentPublishFactIndex(d.d_period) }
  }

}
