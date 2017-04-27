package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import org.apache.commons.lang3.StringUtils

class TestUpdateAssetSnapshotDB extends SparkSpec(null) {

    override def beforeAll() {
    	super.beforeAll();
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.asset_snapshot_summary");
        }
    }
    
    "UpdateAssetSnapshotDB" should "update asset snapshot db" in {

        val rdd = loadFile[DerivedEvent]("src/test/resources/asset-snapshot-updater/test1.log");
        UpdateAssetSnapshotDB.execute(rdd, None)

        val as1 = sc.cassandraTable[AssetSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.ASSET_SNAPSHOT_SUMMARY)

        val DAYas1 = as1.filter { x => x.d_period == 20170425 && StringUtils.equals(x.d_partner_id, "all")}.first()
        val WEEKas1 = as1.filter { x => x.d_period == 2017717 && StringUtils.equals(x.d_partner_id, "all")}.first()
        val MONTHas1 = as1.filter { x => x.d_period == 201704 && StringUtils.equals(x.d_partner_id, "all")}.first()

        DAYas1.d_partner_id should be("all")
        DAYas1.total_activities_count should be(86L)
        DAYas1.total_activities_count should be(DAYas1.total_activities_count_start)
        DAYas1.used_activities_count should be(DAYas1.used_activities_count_start)
        DAYas1.used_activities_count should be(21L)

        DAYas1.total_templates_count should be(615L)
        DAYas1.total_images_count should be(36553L)
        DAYas1.used_images_count should be(29541L)
        DAYas1.used_templates_count should be(289L)
        DAYas1.used_audio_count should be(23257L)
        DAYas1.total_audio_count should be(26824L)
        DAYas1.used_questions_count should be(480L)
        DAYas1.total_questions_count should be(7619L)

        DAYas1.total_templates_count should be(DAYas1.total_templates_count_start)
        DAYas1.total_images_count should be(DAYas1.total_images_count_start)
        DAYas1.used_images_count should be(DAYas1.used_images_count_start)
        DAYas1.used_templates_count should be(DAYas1.used_templates_count_start)
        DAYas1.used_audio_count should be(DAYas1.used_audio_count_start)
        DAYas1.total_audio_count should be(DAYas1.total_audio_count_start)
        DAYas1.used_questions_count should be(DAYas1.used_questions_count_start)
        DAYas1.total_questions_count should be(DAYas1.total_questions_count_start)
        
        
        //Week data
        
        WEEKas1.d_partner_id should be("all")
        WEEKas1.total_activities_count should be(86L)
        WEEKas1.total_activities_count should be(WEEKas1.total_activities_count_start)
        WEEKas1.used_activities_count should be(WEEKas1.used_activities_count_start)
        WEEKas1.used_activities_count should be(21L)

        WEEKas1.total_templates_count should be(615L)
        WEEKas1.total_images_count should be(36553L)
        WEEKas1.used_images_count should be(29541L)
        WEEKas1.used_templates_count should be(289L)
        WEEKas1.used_audio_count should be(23257L)
        WEEKas1.total_audio_count should be(26824L)
        WEEKas1.used_questions_count should be(480L)
        WEEKas1.total_questions_count should be(7619L)

        WEEKas1.total_templates_count should be(WEEKas1.total_templates_count_start)
        WEEKas1.total_images_count should be(WEEKas1.total_images_count_start)
        WEEKas1.used_images_count should be(WEEKas1.used_images_count_start)
        WEEKas1.used_templates_count should be(WEEKas1.used_templates_count_start)
        WEEKas1.used_audio_count should be(WEEKas1.used_audio_count_start)
        WEEKas1.total_audio_count should be(WEEKas1.total_audio_count_start)
        WEEKas1.used_questions_count should be(WEEKas1.used_questions_count_start)
        WEEKas1.total_questions_count should be(WEEKas1.total_questions_count_start)
    }
    
    it should "test asset snapshot data for the mid of a week" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/asset-snapshot-updater/test2.log");
        UpdateAssetSnapshotDB.execute(rdd, None)

        val as = sc.cassandraTable[AssetSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.ASSET_SNAPSHOT_SUMMARY)

        val DAYas = as.filter { x => x.d_period == 20170426 }.first()
        val WEEKas = as.filter { x => x.d_period == 2017717 }.first()
        val MONTHas = as.filter { x => x.d_period == 201704 }.first()
        
        
        
        DAYas.used_images_count should be(29542L)
        DAYas.used_audio_count should be(23258L)
        
        DAYas.total_activities_count should be(DAYas.total_activities_count_start)
        DAYas.used_activities_count should be(DAYas.used_activities_count_start)
        DAYas.total_templates_count should be(DAYas.total_templates_count_start)
        DAYas.total_images_count should be(DAYas.total_images_count_start)
        DAYas.used_images_count should be(DAYas.used_images_count_start)
        DAYas.used_templates_count should be(DAYas.used_templates_count_start)
        DAYas.used_audio_count should be(DAYas.used_audio_count_start)
        DAYas.total_audio_count should be(DAYas.total_audio_count_start)
        DAYas.used_questions_count should be(DAYas.used_questions_count_start)
        DAYas.total_questions_count should be(DAYas.total_questions_count_start)
        
        // Week Data
        
        WEEKas.total_activities_count should be >= (WEEKas.total_activities_count_start)
        WEEKas.used_activities_count should be >= (WEEKas.used_activities_count_start)
        WEEKas.total_templates_count should be >= (WEEKas.total_templates_count_start)
        WEEKas.total_images_count should be >= (WEEKas.total_images_count_start)
        WEEKas.used_images_count should be >= (WEEKas.used_images_count_start)
        WEEKas.used_templates_count should be >= (WEEKas.used_templates_count_start)
        WEEKas.used_audio_count should be >= (WEEKas.used_audio_count_start)
        WEEKas.total_audio_count should be >= (WEEKas.total_audio_count_start)
        WEEKas.used_questions_count should be >= (WEEKas.used_questions_count_start)
        WEEKas.total_questions_count should be >= (WEEKas.total_questions_count_start)
    }
}