package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.ItemUsageSummaryFact
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Period
import org.joda.time.format.DateTimeFormat

class TestUpdatePublishPipelineSummary extends SparkSpec(null) {

  "UpdatePublishPipelineSummary" should "update the content_publish_fact table for DAY, WEEK, MONTH, YEAR" in {

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute("TRUNCATE content_db.content_publish_fact");
    }

    val rdd = loadFile[DerivedEvent]("src/test/resources/pipeline-summary-updater/test_data1.log");
    val rdd2 = UpdatePublishPipelineSummary.execute(rdd, None);

    val rdd3 = loadFile[DerivedEvent]("src/test/resources/pipeline-summary-updater/test_data2.log");
    val rdd4 = UpdatePublishPipelineSummary.execute(rdd3, None);

    val daywiseSummary20170523 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 20170523).collect
    daywiseSummary20170523.size should be(1)
    val summaryFor20170523 = daywiseSummary20170523.head
    summaryFor20170523.draft_count should be(5)
    summaryFor20170523.review_count should be(3)
    summaryFor20170523.live_count should be(4)
    summaryFor20170523.item_created_count should be(3)
    summaryFor20170523.audio_created_count should be(2)
    summaryFor20170523.video_created_count should be(1)
    summaryFor20170523.image_created_count should be(2)
    summaryFor20170523.plugin_created_count should be(2)
    summaryFor20170523.textbook_created_count should be(4)
    summaryFor20170523.textbook_live_count should be(4)

    val daywiseSummary20170524 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 20170524).collect
    daywiseSummary20170524.size should be(1)
    val summaryFor20170524 = daywiseSummary20170524.head
    summaryFor20170524.draft_count should be(23)
    summaryFor20170524.review_count should be(3)
    summaryFor20170524.live_count should be(4)
    summaryFor20170524.item_created_count should be(3)
    summaryFor20170524.audio_created_count should be(2)
    summaryFor20170524.video_created_count should be(100)
    summaryFor20170524.image_created_count should be(2)
    summaryFor20170524.plugin_created_count should be(2)
    summaryFor20170524.textbook_created_count should be(2)
    summaryFor20170524.textbook_live_count should be(4)

    val daywiseSummary20170525 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 20170525).collect
    daywiseSummary20170525.size should be(1)
    val summaryFor20170525 = daywiseSummary20170525.head
    summaryFor20170525.draft_count should be(10)
    summaryFor20170525.review_count should be(3)
    summaryFor20170525.live_count should be(4)
    summaryFor20170525.item_created_count should be(3)
    summaryFor20170525.audio_created_count should be(2)
    summaryFor20170525.video_created_count should be(200)
    summaryFor20170525.image_created_count should be(2)
    summaryFor20170525.plugin_created_count should be(2)
    summaryFor20170525.textbook_created_count should be(3)
    summaryFor20170525.textbook_live_count should be(8)

    val weekwiseSummary = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 2017721).collect
    weekwiseSummary.size should be(1)
    val summaryForWeek = weekwiseSummary.head
    summaryForWeek.draft_count should be(38)
    summaryForWeek.review_count should be(9)
    summaryForWeek.live_count should be(12)
    summaryForWeek.item_created_count should be(9)
    summaryForWeek.audio_created_count should be(6)
    summaryForWeek.video_created_count should be(301)
    summaryForWeek.image_created_count should be(6)
    summaryForWeek.plugin_created_count should be(6)
    summaryForWeek.textbook_created_count should be(9)
    summaryForWeek.textbook_live_count should be(16)

    val monthwiseSummary201705 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 201705).collect
    monthwiseSummary201705.size should be(1)

    val monthwiseSummary201704 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 201704).collect
    monthwiseSummary201704.size should be(1)
    val summaryForMonth201704 = monthwiseSummary201704.head
    summaryForMonth201704.draft_count should be(10)
    summaryForMonth201704.review_count should be(3)
    summaryForMonth201704.live_count should be(4)
    summaryForMonth201704.item_created_count should be(3)
    summaryForMonth201704.audio_created_count should be(2)
    summaryForMonth201704.video_created_count should be(22)
    summaryForMonth201704.image_created_count should be(2)
    summaryForMonth201704.plugin_created_count should be(2)
    summaryForMonth201704.textbook_created_count should be(3)
    summaryForMonth201704.textbook_live_count should be(8)

    val cumulativeSummary = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 0).collect
    cumulativeSummary.size should be(1)
    val s = cumulativeSummary.head
    s.draft_count should be(48)
    s.review_count should be(12)
    s.live_count should be(16)
    s.item_created_count should be(12)
    s.audio_created_count should be(8)
    s.video_created_count should be(323)
    s.image_created_count should be(8)
    s.plugin_created_count should be(8)
    s.textbook_created_count should be(12)
    s.textbook_live_count should be(24)
  }

}