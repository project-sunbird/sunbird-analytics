package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework._

class TestEOCRecommendationFunnelModel extends SparkSpec(null) {

    "EOCRecommendationFunnelModel" should "generate values if event list having GE_SERVICE_API_CALL but eid not equals to contentid" in {

        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(0)
        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_count").get.asInstanceOf[Int] should be(0)
        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_played").get.asInstanceOf[Int] should be(0)
        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_complete").get.asInstanceOf[Int] should be(0)
        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_viewed").get.asInstanceOf[String] should be("")
        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_recommended").get.asInstanceOf[List[String]] should be(List())
        eocEvents(0).dimensions.content_id should be(Some("do_30094761"))
        eocEvents(0).dimensions.did should be(Some("c69fe793-d883-47c4-8de2-96138a30cb96"))
        eocEvents(0).dimensions.content_id should be(Some("do_30094761"))
      

    }

    it should "generate values if event list having GE_SERVICE_API_CALL and OE_INTERACT event gdata id equals to contentid" in {

        eocEvents(1).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(1)
        eocEvents(1).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_count").get.asInstanceOf[Int] should be(2)
        eocEvents(1).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_played").get.asInstanceOf[Int] should be(0)
        eocEvents(1).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_complete").get.asInstanceOf[Int] should be(0)
        eocEvents(1).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_viewed").get.asInstanceOf[String] should be("org.ekstep.ms_52fdbc1e69702d16cd040000")
        eocEvents(1).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_recommended").get.asInstanceOf[List[String]] should be(List("org.ekstep.ms_52fdbc1e69702d16cd040000", "numeracy_365"))
        eocEvents(1).dimensions.content_id should be(Some("do_30094761"))
        eocEvents(1).dimensions.did should be(Some("c69fe793-d883-47c4-8de2-96138a30cb96"))
        eocEvents(1).context.granularity should be("EVENT")
        eocEvents(1).context.date_range.from should be(1489481585164L)
        eocEvents(1).context.date_range.to should be(1491068041249L)
    }

    it should "generate values if content recommendations are present" in {

        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(1)
        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_count").get.asInstanceOf[Int] should be(2)
        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_played").get.asInstanceOf[Int] should be(0)
        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_initiated").get.asInstanceOf[Int] should be(0)
        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_complete").get.asInstanceOf[Int] should be(0)
        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_viewed").get.asInstanceOf[String] should be("org.ekstep.ms_52fdbc1e69702d16cd040000")
        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_recommended").get.asInstanceOf[List[String]] should be(List("org.ekstep.ms_52fdbc1e69702d16cd040000", "numeracy_365"))
        eocEvents(4).dimensions.content_id should be(Some("do_30094761"))
        eocEvents(4).dimensions.did should be(Some("c69fe793-d883-47c4-8de2-96138a30cb96"))
    }

    it should "generate values if content download initiated and completed but not played" in {

        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(1)
        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_count").get.asInstanceOf[Int] should be(2)
        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_played").get.asInstanceOf[Int] should be(0)
        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_initiated").get.asInstanceOf[Int] should be(1)
        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_complete").get.asInstanceOf[Int] should be(1)
        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_viewed").get.asInstanceOf[String] should be("org.ekstep.ms_52fdbc1e69702d16cd040000")
        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_recommended").get.asInstanceOf[List[String]] should be(List("org.ekstep.ms_52fdbc1e69702d16cd040000", "numeracy_365"))
        eocEvents(5).dimensions.content_id should be(Some("do_30094761"))
        eocEvents(5).dimensions.did should be(Some("c69fe793-d883-47c4-8de2-96138a30cb96"))
    }

    it should "generate values if event list content already there and played" in {

        eocEvents(6).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(1)
        eocEvents(6).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_count").get.asInstanceOf[Int] should be(2)
        eocEvents(6).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_played").get.asInstanceOf[Int] should be(1)
        eocEvents(6).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_initiated").get.asInstanceOf[Int] should be(0)
        eocEvents(6).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_complete").get.asInstanceOf[Int] should be(0)
        eocEvents(6).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_viewed").get.asInstanceOf[String] should be("org.ekstep.ms_52fdbc1e69702d16cd040000")
        eocEvents(6).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_recommended").get.asInstanceOf[List[String]] should be(List("org.ekstep.ms_52fdbc1e69702d16cd040000", "numeracy_365"))
        eocEvents(6).dimensions.content_id should be(Some("do_30094761"))
        eocEvents(6).dimensions.did should be(Some("c69fe793-d883-47c4-8de2-96138a30cb96"))
    }

    it should "generate values if content download initiated, completed and played" in {

        eocEvents(7).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(1)
        eocEvents(7).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_count").get.asInstanceOf[Int] should be(2)
        eocEvents(7).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_played").get.asInstanceOf[Int] should be(1)
        eocEvents(7).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_initiated").get.asInstanceOf[Int] should be(1)
        eocEvents(7).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_complete").get.asInstanceOf[Int] should be(1)
        eocEvents(7).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_viewed").get.asInstanceOf[String] should be("org.ekstep.ms_52fdbc1e69702d16cd040000")
        eocEvents(7).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_recommended").get.asInstanceOf[List[String]] should be(List("org.ekstep.ms_52fdbc1e69702d16cd040000", "numeracy_365"))
        eocEvents(7).dimensions.content_id should be(Some("do_30094761"))
        eocEvents(7).dimensions.did should be(Some("c69fe793-d883-47c4-8de2-96138a30cb96"))
    }
    
    it should "generate values if content download initiated, not completed" in {

        eocEvents(8).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(1)
        eocEvents(8).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_count").get.asInstanceOf[Int] should be(2)
        eocEvents(8).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_played").get.asInstanceOf[Int] should be(0)
        eocEvents(8).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_initiated").get.asInstanceOf[Int] should be(1)
        eocEvents(8).edata.eks.asInstanceOf[Map[String, AnyRef]].get("download_complete").get.asInstanceOf[Int] should be(0)
        eocEvents(8).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_viewed").get.asInstanceOf[String] should be("org.ekstep.ms_52fdbc1e69702d16cd040000")
        eocEvents(8).edata.eks.asInstanceOf[Map[String, AnyRef]].get("content_recommended").get.asInstanceOf[List[String]] should be(List("org.ekstep.ms_52fdbc1e69702d16cd040000", "numeracy_365"))
        eocEvents(8).dimensions.content_id should be(Some("do_30094761"))
        eocEvents(8).dimensions.did should be(Some("c69fe793-d883-47c4-8de2-96138a30cb96"))
    }

    it should "run the funnel summarizer on telemetry and produce EOC funnels" in {

        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-eoc-funnel-data.json.gz");
        val rdd2 = EOCRecommendationFunnelModel.execute(rdd, None);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "logs/output.log")), rdd2);
        rdd2.count() should be(69);
    }

    private def eocEvents: Array[MeasuredEvent] = {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/EOC_Test.log");
        val rdd2 = EOCRecommendationFunnelModel.execute(rdd, None);
        rdd2.collect
    }

}