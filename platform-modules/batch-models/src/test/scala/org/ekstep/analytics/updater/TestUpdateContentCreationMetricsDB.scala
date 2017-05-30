package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.DBUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.model.SparkGraphSpec
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.creation.model.CreationEvent

class TestUpdateContentCreationMetricsDB extends SparkGraphSpec(null) {

    "UpdateContentCreationMetricsDB" should "take the snapshot data for the content having no plugins and no tags update in DB" in {

        DBUtil.truncateTable(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE)
        loadGraphData("src/test/resources/content-creation-metrics/graph-data.json")
        val rdd = loadFile[CreationEvent]("src/test/resources/pipeline-summary/test_data1.log");
        UpdateContentCreationMetricsDB.execute(rdd, None)
        val metrics = sc.cassandraTable[ContentCreationMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE).collect
        metrics.length should be(3)

        metrics.map { x => x.plugin_metrics }.foreach { x =>
            x.isEmpty should be(true)
        }

        val cnt1 = metrics.filter { x => StringUtils.equals("org.ekstep.ra_ms_52d02eae69702d0905cf0800", x.d_content_id) }.last
        cnt1.audios_count should be(7)
        cnt1.images_count should be(10)
        cnt1.videos_count should be(0)
        
        cnt1.pkg_version should be (2)
        cnt1.tags_count should be (0)

        val cnt2 = metrics.filter { x => StringUtils.equals("org.ekstep.ra_ms_5391b1d669702d107e030000", x.d_content_id) }.last
        cnt2.audios_count should be(15)
        cnt2.images_count should be(10)
        cnt2.videos_count should be(0)
        
        cnt2.pkg_version should be (6)
        cnt2.tags_count should be (0)

        val cnt3 = metrics.filter { x => StringUtils.equals("org.ekstep.ra_ms_52d058e969702d5fe1ae0f00", x.d_content_id) }.last
        cnt3.audios_count should be(8)
        cnt3.images_count should be(11)
        cnt3.videos_count should be(0)
        
        cnt3.pkg_version should be (3)
        cnt3.tags_count should be (0)

    }

    it should "check for non empty plugin metrics and one content related tag" in {
        
        DBUtil.truncateTable(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE);
        DBUtil.truncateTable(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE);
        loadCassandraData(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE, "src/test/resources/content-creation-metrics/content_data_test.txt", ";")
        loadGraphData("src/test/resources/content-creation-metrics/graph-data1.json")
        
        val relationQuery = "MATCH (cnt:domain {IL_UNIQUE_ID:'do_2122040066659860481139'}),(tag:domain {IL_UNIQUE_ID:'TAG_english_stories'}) CREATE (tag)-[r:hasMember]->(cnt) RETURN r"
        executeQueries(List(relationQuery))
        val rdd = loadFile[CreationEvent]("src/test/resources/pipeline-summary/test_data1.log");
        UpdateContentCreationMetricsDB.execute(rdd, None)
        val metrics = sc.cassandraTable[ContentCreationMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE).collect
        metrics.length should be(2)

        metrics.map { x => x.plugin_metrics }.foreach { x =>
            x.nonEmpty should be (true)
        }
        
        val cnt1 = metrics.filter { x => StringUtils.equals("do_2122040066659860481139", x.d_content_id) }.last
        cnt1.audios_count should be(0)
        cnt1.images_count should be(1)
        cnt1.videos_count should be(1)
        cnt1.pkg_version should be (6)
        cnt1.tags_count should be (1)
        
        val cnt2 = metrics.filter { x => StringUtils.equals("do_112238916211949568137", x.d_content_id) }.last
        cnt2.audios_count should be(0)
        cnt2.images_count should be(3)
        cnt2.videos_count should be(0)
        cnt2.pkg_version should be (3)
        cnt2.tags_count should be (0)
    }
}