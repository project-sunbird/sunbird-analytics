package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.util.DBUtil
import org.ekstep.analytics.util.Constants

class TestAssetSnapshotSummaryModel extends SparkGraphSpec(null) {

    override def beforeAll() {
        super.beforeAll()
        DBUtil.truncateTable(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE);
        DBUtil.importContentData(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE, "src/test/resources/vidyavaani-data/content_data.csv", ",");
    }
    
    "AssetSnapshotSummaryModel" should "generate Asset snapshot and compare between the total values and used value" in {

        ContentAssetRelationModel.main("{}")(Option(sc));

        val event = AssetSnapshotSummaryModel.execute(sc.makeRDD(List()), None).collect.last

        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]

        val totalImageCount = eksMap.get("total_images_count").get.asInstanceOf[Long]
        val usedImageCount = eksMap.get("used_images_count").get.asInstanceOf[Long]
        val totalAudioCount = eksMap.get("total_audio_count").get.asInstanceOf[Long]
        val usedAudioCount = eksMap.get("used_audio_count").get.asInstanceOf[Long]
        val totalQCount = eksMap.get("total_questions_count").get.asInstanceOf[Long]
        val usedQCount = eksMap.get("used_questions_count").get.asInstanceOf[Long]
        val totalActCount = eksMap.get("total_activities_count").get.asInstanceOf[Long]
        val usedActCount = eksMap.get("used_activities_count").get.asInstanceOf[Long]
        val totalTempCount = eksMap.get("total_templates_count").get.asInstanceOf[Long]
        val usedTempCount = eksMap.get("used_templates_count").get.asInstanceOf[Long]

        totalImageCount should be >= usedImageCount
        totalAudioCount should be >= usedAudioCount
        totalQCount should be >= usedQCount
        totalActCount should be >= usedActCount
        totalTempCount should be >= usedTempCount

        totalImageCount should be(4L)
        usedImageCount should be(4L)
    }
}