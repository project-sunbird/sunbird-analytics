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

class TestUpdateItemSummaryDB extends SparkSpec(null) {

    "UpdateItemSummaryDB" should "update the item usage summary db and check the all fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.item_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-updater/ius_1.log");
        val rdd2 = UpdateItemSummaryDB.execute(rdd, None);

        val cummAlldo_20043159ItemSumm = sc.cassandraTable[ItemUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT).where("d_period=?", 0).where("d_tag=?", "all").where("d_content_id=?", "do_20043159").collect
        cummAlldo_20043159ItemSumm.size should be(4)

        val Q1 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q1", x.d_item_id) }.last

        Q1.m_total_ts should be(3)
        Q1.m_total_count should be(1)
        Q1.m_avg_ts should be(3)
        Q1.m_correct_res_count should be(0)
        Q1.m_inc_res_count should be(1)
        Q1.m_correct_res should be(List())

        val Q2 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q2", x.d_item_id) }.last

        Q2.m_total_ts should be(2)
        Q2.m_total_count should be(1)
        Q2.m_correct_res_count should be(0)
        Q2.m_inc_res_count should be(1)
        Q2.m_correct_res should be(List())

        val Q3 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q3", x.d_item_id) }.last

        Q3.m_total_ts should be(3)
        Q3.m_total_count should be(1)
        Q3.m_avg_ts should be(3)
        Q3.m_correct_res_count should be(1)
        Q3.m_inc_res_count should be(0)
        Q3.m_top5_incorrect_res should be(List())

        val Q4 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q4", x.d_item_id) }.last

        Q4.m_total_ts should be(2)
        Q4.m_total_count should be(2)
        Q4.m_avg_ts should be(1)
        Q4.m_correct_res_count should be(0)
        Q4.m_inc_res_count should be(2)

    }

}