package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event;
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.creation.model.CreationEvent

class TestPublishPipelineSummaryModel extends SparkSpec(null) {

  "PipelineSummaryModel" should "generate pipeline summary" in {
    val rdd = loadFile[CreationEvent]("src/test/resources/pipeline-summary/test_data1.log");
    val result = PublishPipelineSummaryModel.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "PublishPipelineSummarizer")));
    val me = result.collect();
    me.length should be(3)

    me.foreach { e =>
      e.eid should be("ME_PUBLISH_PIPELINE_SUMMARY")
      val context = e.context;
      context.pdata.id should be("AnalyticsDataPipeline")
      context.pdata.model should be("PublishPipelineSummarizer")
      context.pdata.ver should be("1.0")
      context.granularity should be("DAY")
      e.syncts shouldNot be(0)
      e.uid should be("")
    }

    val summaryFor21May = me.find(e => e.dimensions.period == Option(20170521)).get
    assert_expected(e = summaryFor21May, draft_count = 1, video_created_count = 1)

    val summaryFor22May = me.find(e => e.dimensions.period == Option(20170522)).get
    assert_expected(e = summaryFor22May, draft_count = 1)

    val summaryFor23May = me.find(e => e.dimensions.period == Option(20170523)).get
    assert_expected(e = summaryFor23May, draft_count = 2, review_count = 3, live_count = 4,
      item_created_count = 3, audio_created_count = 2, video_created_count = 1,
      image_created_count = 2, plugin_created_count = 2, textbook_created_count = 2,
      textbook_live_count = 4)
  }

  def assert_expected(e: MeasuredEvent, draft_count: Int = 0, review_count: Int = 0, live_count: Int = 0,
                      item_created_count: Int = 0, audio_created_count: Int = 0, video_created_count: Int = 0,
                      image_created_count: Int = 0, plugin_created_count: Int = 0, textbook_created_count: Int = 0,
                      textbook_live_count: Int = 0) {

    val eks = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(e.edata.eks));
    val summary = eks("publish_pipeline_summary").asInstanceOf[List[Map[String, AnyRef]]]
    if (draft_count > 0) { summary.filter(s => s("type").toString() == "Content" && s("state").toString() == "Draft").head("count").asInstanceOf[Int] should be(draft_count) }
    if (review_count > 0) { summary.filter(s => s("type").toString() == "Content" && s("state").toString() == "Review").head("count").asInstanceOf[Int] should be(review_count) }
    if (live_count > 0) { summary.filter(s => s("type").toString() == "Content" && s("state").toString() == "Live").head("count").asInstanceOf[Int] should be(live_count) }

    if (item_created_count > 0) { summary.filter(s => s("type").toString() == "Item" && s("state").toString() == "Live").head("count").asInstanceOf[Int] should be(item_created_count) }

    if (audio_created_count > 0) { summary.filter(s => s("type").toString() == "Asset" && s("subtype").toString() == "audio" && s("state").toString() == "Live").head("count").asInstanceOf[Int] should be(audio_created_count) }
    if (video_created_count > 0) { summary.filter(s => s("type").toString() == "Asset" && s("subtype").toString() == "video" && s("state").toString() == "Live").head("count").asInstanceOf[Int] should be(video_created_count) }
    if (image_created_count > 0) { summary.filter(s => s("type").toString() == "Asset" && s("subtype").toString() == "image" && s("state").toString() == "Live").head("count").asInstanceOf[Int] should be(image_created_count) }

    if (plugin_created_count > 0) { summary.filter(s => s("type").toString() == "Plugin" && s("subtype").toString() == "" && s("state").toString() == "Live").head("count").asInstanceOf[Int] should be(plugin_created_count) }

    if (textbook_created_count > 0) { summary.filter(s => s("type").toString() == "Textbook" && s("subtype").toString() == "" && s("state").toString() == "Draft").head("count").asInstanceOf[Int] should be(textbook_created_count) }
    if (textbook_live_count > 0) { summary.filter(s => s("type").toString() == "Textbook" && s("subtype").toString() == "" && s("state").toString() == "Live").head("count").asInstanceOf[Int] should be(textbook_live_count) }
  }
}

