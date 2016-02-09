package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil

class TestRecoEngine extends SparkSpec(null) {

    "RecoEngine" should " print Pij value per learner per concept" in {
        
        val rdd = loadFile[MeasuredEvent]("src/test/resources/reco-engine/reco_engine_test.log");
        val rdd2 = RecoEngine.execute(sc, rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "RecoEngine")));
        val result = rdd2.collect();
        result.foreach {
            println(_);
        }
        /*val relevance = sc.cassandraTable[Relevance]("learner_db", "conceptrelevance").first();
        val r = relevance.relevance.map(f => f._2);
        println(r.max, r.min, CommonUtil.roundDouble(r.sum, 3));
        r.size should be(323)*/
        //OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/reco-engine/test_output.log")), rdd2);
    }
}