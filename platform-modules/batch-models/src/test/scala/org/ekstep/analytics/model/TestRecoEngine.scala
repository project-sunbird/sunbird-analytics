package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent

class TestRecoEngine extends SparkSpec(null) {

    "RecoEngine" should " print Pij value per learner per concept" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/reco-engine/generic_session_summary_out.log");
        val rdd2 = RecoEngine.execute(sc, rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "RecoEngine")));
        val pijValues = rdd2.collect()
        for (pij <- pijValues) {
            println(pij)
        }
        //OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/learner_activity_test_output.log")), rdd2);
    }
}