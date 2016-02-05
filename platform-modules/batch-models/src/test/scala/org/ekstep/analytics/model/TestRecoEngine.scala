package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import com.datastax.spark.connector._

class TestRecoEngine extends SparkSpec(null) {

    "RecoEngine" should " print Pij value per learner per concept" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/reco-engine/reco_engine_test.log");
        val rdd2 = RecoEngine.execute(sc, rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "RecoEngine")));

        val relevance = sc.cassandraTable[Relevance]("learner_db", "conceptrelevance").map { x => (x.learner_id, x.relevance) };
        relevance.count should be(1)
        val r = relevance.collect()(0)._2
        val value = r.values
        Math.round(value.sum) should be (1)
        value.size should be (323)
        //OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/reco-engine/test_output.log")), rdd2);
    }
}