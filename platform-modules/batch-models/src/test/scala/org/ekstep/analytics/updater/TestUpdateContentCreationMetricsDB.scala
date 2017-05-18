package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.DBUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.model.SparkGraphSpec
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.lang3.StringUtils

class TestUpdateContentCreationMetricsDB extends SparkGraphSpec(null) {

    "UpdateContentCreationMetricsDB" should "take the snapshot data and update to DB" in {

        UpdateContentCreationMetricsDB.execute(sc.makeRDD(Seq(Empty())), None)
        val metrics = sc.cassandraTable[ContentCreationMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE).collect
        metrics.length should be(3)

        metrics.map { x => x.plugin_metrics }.foreach { x =>
            x.isEmpty should be(true)
        }

        metrics.foreach { x =>
            x.tags should be(0)
            x.live_times should be(0)
        }

        val cnt1 = metrics.filter { x => StringUtils.equals("org.ekstep.ra_ms_52d02eae69702d0905cf0800", x.d_content_id) }.last
        cnt1.audios should be(7)
        cnt1.images should be(10)
        cnt1.videos should be(0)

        val cnt2 = metrics.filter { x => StringUtils.equals("org.ekstep.ra_ms_5391b1d669702d107e030000", x.d_content_id) }.last
        cnt2.audios should be(15)
        cnt2.images should be(10)
        cnt2.videos should be(0)

        val cnt3 = metrics.filter { x => StringUtils.equals("org.ekstep.ra_ms_52d058e969702d5fe1ae0f00", x.d_content_id) }.last
        cnt3.audios should be(8)
        cnt3.images should be(11)
        cnt3.videos should be(0)

    }

    it should "check for the plugin metrics" in {
        DBUtil.truncateTable(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE)
        DBUtil.truncateTable(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE)
        DBUtil.importContentData(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE, "src/test/resources/content-creation-metrics/content_data_test.txt",";")

        UpdateContentCreationMetricsDB.execute(sc.makeRDD(Seq(Empty())), None)
        val metrics = sc.cassandraTable[ContentCreationMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE).collect
        metrics.length should be(2)

        metrics.map { x => x.plugin_metrics }.foreach { x =>
            x.nonEmpty should be (true)
        }
        
        val cnt1 = metrics.filter { x => StringUtils.equals("do_2122040066659860481139", x.d_content_id) }.last
        cnt1.audios should be(0)
        cnt1.images should be(1)
        cnt1.videos should be(1)

        val cnt2 = metrics.filter { x => StringUtils.equals("do_112238916211949568137", x.d_content_id) }.last
        cnt2.audios should be(0)
        cnt2.images should be(3)
        cnt2.videos should be(0)
    }
}