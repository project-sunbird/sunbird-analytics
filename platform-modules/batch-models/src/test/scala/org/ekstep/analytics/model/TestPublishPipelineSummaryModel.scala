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
    println(JSONUtils.serialize(me.last))
    me.length should be(3)

    me.foreach {e =>
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

    val eks = JSONUtils.deserialize[PipelineSummaryOutput](JSONUtils.serialize(e.edata.eks));
    val cp = eks.content_publish
    cp.draft_count should be(draft_count)
    cp.review_count should be(review_count)
    cp.live_count should be(live_count)

    val ip = eks.item_publish
    ip.created_count should be(item_created_count)

    val ap = eks.asset_publish
    ap.audio_created_count should be(audio_created_count)
    ap.video_created_count should be(video_created_count)
    ap.image_created_count should be(image_created_count)
    ap.plugin_created_count should be(plugin_created_count)

    val tp = eks.textbook_publish
    tp.created_count should be(textbook_created_count)
    tp.live_count should be(textbook_live_count)


  }
}

