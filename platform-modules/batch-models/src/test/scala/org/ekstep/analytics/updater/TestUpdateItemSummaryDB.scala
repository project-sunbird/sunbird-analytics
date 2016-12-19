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

class TestUpdateItemSummaryDB extends SparkSpec(null) {

    "UpdateItemSummaryDB" should "update the item usage summary db and check the all fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.item_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-updater/ius_2.log");
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
        Q1.m_incorrect_res.last._1 should be("2:3")

        val Q2 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q2", x.d_item_id) }.last

        Q2.m_total_ts should be(2)
        Q2.m_total_count should be(1)
        Q2.m_correct_res_count should be(0)
        Q2.m_inc_res_count should be(1)
        Q2.m_correct_res should be(List())
        Q2.m_incorrect_res.last._1 should be("2:25")

        val Q3 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q3", x.d_item_id) }.last

        Q3.m_total_ts should be(3)
        Q3.m_total_count should be(1)
        Q3.m_avg_ts should be(3)
        Q3.m_correct_res_count should be(1)
        Q3.m_inc_res_count should be(0)
        Q3.m_top5_incorrect_res should be(List())
        Q3.m_correct_res.last should be("2:18")

        val Q4 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q4", x.d_item_id) }.last

        Q4.m_total_ts should be(4)
        Q4.m_total_count should be(4)
        Q4.m_avg_ts should be(1)
        Q4.m_correct_res_count should be(0)
        Q4.m_inc_res_count should be(4)
    }

    it should "test correct and incorrect result aggregation" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.item_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-updater/us20.log");
        val rdd2 = UpdateItemSummaryDB.execute(rdd, None);

        val itemData = sc.cassandraTable[ItemUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT).collect

        val week41 = itemData.filter { x => ("all".equals(x.d_tag)) && ("org.ekstep.delta".equals(x.d_content_id) && 2016741 == x.d_period && ("single-add".equals(x.d_item_id))) }

        val oct10 = itemData.filter { x => ("all".equals(x.d_tag)) && ("org.ekstep.delta".equals(x.d_content_id) && 20161010 == x.d_period && ("single-add".equals(x.d_item_id))) }.last
        val oct11 = itemData.filter { x => ("all".equals(x.d_tag)) && ("org.ekstep.delta".equals(x.d_content_id) && 20161011 == x.d_period && ("single-add".equals(x.d_item_id))) }.last

        val week41CorrectRes = week41.flatMap { x => x.m_correct_res }.distinct

        val aggregateValue = (oct10.m_correct_res ++ oct11.m_correct_res).distinct
        aggregateValue.size should be(week41CorrectRes.size)

        JSONUtils.serialize(oct10.m_correct_res) should be("""["6","5","2"]""")
        JSONUtils.serialize(oct11.m_correct_res) should be("""["2","4","8","5"]""")

        JSONUtils.serialize(aggregateValue) should be("""["6","5","2","4","8"]""")
        JSONUtils.serialize(week41CorrectRes) should be("""["6","5","2","4","8"]""")
    }

    it should "update the item usage summary db and check newly added fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.item_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-updater/ius_2.log");
        val rdd2 = UpdateItemSummaryDB.execute(rdd, None);

        val cummAlldo_20043159ItemSumm = sc.cassandraTable[ItemUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT).where("d_period=?", 20160928).where("d_tag=?", "all").where("d_content_id=?", "do_20043159").collect

        val Q1 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q1", x.d_item_id) }.last

        JSONUtils.serialize(Q1.m_top5_mmc) should be("""[["m6",6]]""")
        JSONUtils.serialize(Q1.m_mmc) should be("""[["m6",6]]""")

        val Q4 = cummAlldo_20043159ItemSumm.filter { x => StringUtils.equals("Q4", x.d_item_id) }.last

        JSONUtils.serialize(Q4.m_top5_mmc) should be("""[["m4",4],["m6",4]]""")
        JSONUtils.serialize(Q4.m_mmc) should be("""[["m4",4],["m6",4]]""")
        JSONUtils.serialize(Q4.m_mmc_res) should be("""[["m6,m4",6]]""")
    }

    it should "validate top5_misconception per day results from cassandra DB " in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.item_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-updater/us20.log");
        val rdd2 = UpdateItemSummaryDB.execute(rdd, None);

        val itemData = sc.cassandraTable[ItemUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT).collect
        val sept19 = itemData.filter { x => ("all".equals(x.d_tag)) && ("org.ekstep.delta".equals(x.d_content_id) && 20160919 == x.d_period && ("single-add".equals(x.d_item_id))) }
        JSONUtils.serialize(sept19(0).m_top5_mmc) should be("""[["m7",99],["m5",7],["m9",4],["m8",3],["m4",2]]""")
    }

    it should "validate top5_misconception per week results from cassandra DB " in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.item_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-updater/us20.log");
        val rdd2 = UpdateItemSummaryDB.execute(rdd, None);

        val itemData = sc.cassandraTable[ItemUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT).collect
        val week41 = itemData.filter { x => ("all".equals(x.d_tag)) && ("org.ekstep.delta".equals(x.d_content_id) && 2016741 == x.d_period && ("single-add".equals(x.d_item_id))) }
        JSONUtils.serialize(week41(0).m_top5_mmc) should be("""[["m15",100],["m10",23],["m3",9],["m2",5],["m1",3]]""")
    }

}