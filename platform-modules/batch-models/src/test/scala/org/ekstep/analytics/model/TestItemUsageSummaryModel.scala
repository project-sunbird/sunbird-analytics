package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher

class TestItemUsageSummaryModel extends SparkSpec(null) {

    "ItemUsageSummaryModel" should "generate item summaries" in {

        val rdd = loadFile[org.ekstep.analytics.util.DerivedEvent]("src/test/resources/item-usage-summary/test-data.log");

        val rdd2 = ItemUsageSummaryModel.execute(rdd, None);
        val me = rdd2.map { x => JSONUtils.serialize(x) }.collect();
        
        me.length should be(32);
        
        val event = JSONUtils.deserialize[DerivedEvent](me(7));
        event.eid should be("ME_ITEM_SUMMARY")
        event.syncts should be(1474356964993L)
        event.ver should be("1.0")
        event.mid should be("34E364B2E1D30B91D5D9CF062D51EC2E")
        event.uid should be("0d254525-2911-411b-9bb4-4351eab2d916")
        event.context.granularity should be("EVENT")
        event.dimensions.anonymous_user.get should be (false);
        event.dimensions.group_user.get should be (false);
        event.dimensions.ss_mid.get should be ("1D30C7192907AE13012B49EA76446827");
        
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("itemId").get.asInstanceOf[String] should be("akshara.gka.44")
        eksMap.get("score").get.asInstanceOf[Int] should be(1)
        eksMap.get("pass").get.asInstanceOf[String] should be("Yes")
        val res = eksMap.get("res").get.asInstanceOf[List[String]]
        val resValue = eksMap.get("resValues").get.asInstanceOf[List[Object]]
        res.length should be(1)
        resValue.length should be(1)
        res should be (List("0:56"))
        resValue should be (List(Map("0" -> "56")))
        eksMap.get("mc").get.asInstanceOf[List[AnyRef]].length should be(0)
        eksMap.get("time_stamp").get.asInstanceOf[Long] should be(1473923942941L)
    }
    it should "generate None for qtitle and qdesc when raw telemetry don't have qtitle and qdesc" in {
        val rdd = loadFile[org.ekstep.analytics.util.DerivedEvent]("src/test/resources/item-usage-summary/test-data.log");
        val rdd2 = ItemUsageSummaryModel.execute(rdd, None);
        val me = rdd2.map { x => JSONUtils.serialize(x) }.collect();
        val event = JSONUtils.deserialize[DerivedEvent](me(7));
        val itemRes = JSONUtils.deserialize[ItemResponse](JSONUtils.serialize(event.edata.eks))
        itemRes.qtitle should be(None)
        itemRes.qdesc should be(None)
    }

    it should "generate title for qtitle and description for qdesc when raw telemetry having qtitle as title and qdesc as description " in {
        val rdd = loadFile[org.ekstep.analytics.util.DerivedEvent]("src/test/resources/item-usage-summary/test-data1.log");
        val rdd2 = ItemUsageSummaryModel.execute(rdd, None);
        val me = rdd2.map { x => JSONUtils.serialize(x) }.collect();
        val event = JSONUtils.deserialize[DerivedEvent](me(7));
        val itemRes = JSONUtils.deserialize[ItemResponse](JSONUtils.serialize(event.edata.eks))
        itemRes.qtitle.get should be("title")
        itemRes.qdesc.get should be("description")
  }
}