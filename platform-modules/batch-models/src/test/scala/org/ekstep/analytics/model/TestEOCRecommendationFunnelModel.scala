package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Dispatcher

class TestEOCRecommendationFunnelModel extends SparkSpec(null) {

    "EOCRecommendationFunnelModel" should "generate empty values if events having only OE_START and OE_END " in {
       
        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(0)
    }

    it should "show consumed value as 1 if events having GE_SERVICE_API_CALL in between OE_START and OE_END" in {

        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("consumed").get.asInstanceOf[Int] should be(1)
    }

    it should "show Contentlist values if events having OE_INTERACT in between OE_START and OE_END" in {

        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentRecommended").get.asInstanceOf[List[String]](1) should be("numeracy_365")
    }

    it should "show download initiated value as 1 if content download is initiated" in {

        eocEvents(3).edata.eks.asInstanceOf[Map[String, AnyRef]].get("downloadInitiated").get.asInstanceOf[Int] should be(1)
    }

    it should "show download complete value as 1 if content download is completed" in {

        eocEvents(3).edata.eks.asInstanceOf[Map[String, AnyRef]].get("downloadComplete").get.asInstanceOf[Int] should be(1)
    }

    it should "show download complete value as 0 if content download is initiated and not yet completed" in {

        eocEvents(4).edata.eks.asInstanceOf[Map[String, AnyRef]].get("downloadComplete").get.asInstanceOf[Int] should be(0)
    }

    it should "show content played value as 0 if content is not played" in {
        eocEvents(5).edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentPlayed").get.asInstanceOf[Int] should be(0)
    }

    it should "show content played value as 1 if content is played" in {

        eocEvents(0).edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentPlayed").get.asInstanceOf[Int] should be(1)
    }

    private def eocEvents: Array[MeasuredEvent] = {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/EOC_Test4.log");
        val rdd2 = EOCRecommendationFunnelModel.execute(rdd, None);
        rdd2.collect
    }

}