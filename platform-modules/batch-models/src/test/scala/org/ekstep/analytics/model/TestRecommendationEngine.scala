package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil

class TestRecommendationEngine extends SparkSpec(null) {

    "RecommendationEngine" should "run the recommendation for a push data to learner db" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/reco-engine/reco_engine_test.log");
        val rdd2 = RecommendationEngine.execute(sc, rdd, None);
        val result = rdd2.collect();
    }
    
    it should "compute the recommendation for a learner" in {
        
        val learner_id = "8f111d6c-b618-4cf4-bb4b-bbf06cf4363a";
        val lr = LearnerConceptRelevance(learner_id, Map());
        sc.parallelize(Array(lr), 1).saveToCassandra("learner_db", "learnerconceptrelevance");
        
        val rdd = loadFile[MeasuredEvent]("src/test/resources/reco-engine/reco_test_data_1.log");
        val rdd2 = RecommendationEngine.execute(sc, rdd, None);
        val result = rdd2.collect();
        //println(result(0));
        
        val lcr = sc.cassandraTable[LearnerConceptRelevance]("learner_db", "learnerconceptrelevance").where("learner_id = ?", learner_id).first();
        val r = lcr.relevance;
        r.size should be > 100;
    }
}