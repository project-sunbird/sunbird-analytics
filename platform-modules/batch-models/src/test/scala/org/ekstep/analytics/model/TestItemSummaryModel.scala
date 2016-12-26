package org.ekstep.analytics.model

import org.ekstep.analytics.framework.RegisteredTag
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.updater.UpdateItemSummaryDB
import java.io.File
import org.ekstep.analytics.framework.InCorrectRes

class TestItemSummaryModel extends SparkSpec(null) {

    "ItemSummaryModel" should "generates item summary and test all the fields" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.registered_tags");
        }

        val tag1 = RegisteredTag("c6ed6e6849303c77c0182a282ebf318aad28f8d1", System.currentTimeMillis(), true)
        val tag2 = RegisteredTag("e4d7a0063b665b7a718e8f7e4014e59e28642f8c", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1, tag2)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-model/item_summary_1.log");
        val out = ItemSummaryModel.execute(rdd, None);
        val events = out.collect()

        val do_30032995Events = events.filter { x => StringUtils.equals("all", x.dimensions.tag.get) && StringUtils.equals("do_30032995", x.dimensions.content_id.get) && 20160928 == x.dimensions.period.get }
        do_30032995Events.length should be(41)
        val item1 = do_30032995Events.filter { x => StringUtils.equals("ek.n.ib.en.ad.T.167", x.dimensions.item_id.get) }.last

        item1.eid should be("ME_ITEM_USAGE_SUMMARY")
        item1.mid should be("31BA32C3F6AC6EA7E2E4C517C28A5168")

        val item1EksMap = item1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        item1EksMap.get("inc_res_count").get.asInstanceOf[Int] should be(4)
        item1EksMap.get("correct_res_count").get.asInstanceOf[Int] should be(0)
        item1EksMap.get("total_count").get.asInstanceOf[Int] should be(4)
        item1EksMap.get("total_ts").get.asInstanceOf[Double] should be(7)
        item1EksMap.get("avg_ts").get.asInstanceOf[Double] should be(1.75)

    }

    it should "generate item summary from the input data having one pre-registered tag" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-model/item_summary_2.log");
        val out = ItemSummaryModel.execute(rdd, None);
        val events = out.collect()

        val tag1Events = events.filter { x => StringUtils.equals("e4d7a0063b665b7a718e8f7e4014e59e28642f8c", x.dimensions.tag.get) && 20160929 == x.dimensions.period.get }
        tag1Events.length should be(5)
        tag1Events.filter { x => StringUtils.equals("domain_4083", x.dimensions.content_id.get) }.length should be(5)

        val domain_4564Event = tag1Events.filter { x => StringUtils.equals("domain_4564", x.dimensions.item_id.get) }.last

        domain_4564Event.eid should be("ME_ITEM_USAGE_SUMMARY")
        domain_4564Event.mid should be("DB4E2DEB395AA0DAB10896E29E97DF82")

        val domain_4564EventEksMap = domain_4564Event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val incResCount = domain_4564EventEksMap.get("inc_res_count").get.asInstanceOf[Int] 
        incResCount should be(1)
        domain_4564EventEksMap.get("correct_res_count").get.asInstanceOf[Int] should be(1)
        domain_4564EventEksMap.get("total_count").get.asInstanceOf[Int] should be(2)
        domain_4564EventEksMap.get("total_ts").get.asInstanceOf[Double] should be(14)
        domain_4564EventEksMap.get("avg_ts").get.asInstanceOf[Double] should be(7)
        
        val correctRes = domain_4564EventEksMap.get("correct_res").get.asInstanceOf[List[String]]
        correctRes.size should be (1)
        correctRes.last should be ("1:अपनी परछाई")
        
        val incorrectRes = domain_4564EventEksMap.get("incorrect_res").get.asInstanceOf[List[InCorrectRes]];
        incorrectRes.size should be (incResCount)
    }
    
    it should "generate empty Map if  mmc values are not present in Learner Session Summary" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-model/item_summary_3.log");
        val out = ItemSummaryModel.execute(rdd, None);
        val events = out.collect()
        val tag1Events = events.filter { x => StringUtils.equals("e4d7a0063b665b7a718e8f7e4014e59e28642f8c", x.dimensions.tag.get) && 20160929 == x.dimensions.period.get }
        val domain_4564Event = tag1Events.filter { x => StringUtils.equals("domain_4564", x.dimensions.item_id.get) }.last
        val domain_4564EventEksMap = domain_4564Event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        domain_4564EventEksMap.get("incorrect_res").get.asInstanceOf[List[InCorrectRes]] should be(List(InCorrectRes("0:मछली", List(), 1)))
    }

    it should "generate aggregated mmc response when grouped by ItemKey" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-model/item_summary_3.log");
        val out = ItemSummaryModel.execute(rdd, None);
        val events = out.collect()
        val tag1Events = events.filter { x => StringUtils.equals("e4d7a0063b665b7a718e8f7e4014e59e28642f8c", x.dimensions.tag.get) && 20160929 == x.dimensions.period.get }
        val domain_4564Event = tag1Events.filter { x => StringUtils.equals("domain_4544", x.dimensions.item_id.get) }.last
        val domain_4564EventEksMap = domain_4564Event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val maping = domain_4564EventEksMap.get("incorrect_res").get.asInstanceOf[List[InCorrectRes]]
        maping.length should be (2)
    }
    // Mahesh    
    it should "generate response if  mmc values are present in Learner Session Summary" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/item-summary-model/item_summary_3.log");
        val out = ItemSummaryModel.execute(rdd, None);
        val events = out.collect()
        val tag1Events = events.filter { x => StringUtils.equals("e4d7a0063b665b7a718e8f7e4014e59e28642f8c", x.dimensions.tag.get) && 20160929 == x.dimensions.period.get }
        val domain_4564Event = tag1Events.filter { x => StringUtils.equals("domain_4544", x.dimensions.item_id.get) }.last
        val domain_4564EventEksMap = domain_4564Event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val maping = domain_4564EventEksMap.get("incorrect_res").get.asInstanceOf[List[InCorrectRes]]
        maping.length should be (2)
    }
}