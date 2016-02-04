package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher

class TestRecoEngine extends SparkSpec(null) {

    "RecoEngine" should " print Pij value per learner per concept" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/reco-engine/reco_engine_test.log");
        val rdd2 = RecoEngine.execute(sc, rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "RecoEngine")));
//        val me = rdd2.collect()
//        for (pij <- pijValues) {
//            println(pij)
//        }
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/reco-engine/test_output.log")), rdd2);
    }
}