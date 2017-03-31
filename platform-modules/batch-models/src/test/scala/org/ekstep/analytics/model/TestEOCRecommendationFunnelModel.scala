package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Dispatcher

class TestEOCRecommendationFunnelModel extends SparkSpec(null) {

    "EOCRecommendationFunnelModel" should "generate events" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/EOC_Test2.log");
        val rdd2 = EOCRecommendationFunnelModel.execute(rdd, None);
        val events = rdd2.collect
        // events.size should be(73)
    }
}