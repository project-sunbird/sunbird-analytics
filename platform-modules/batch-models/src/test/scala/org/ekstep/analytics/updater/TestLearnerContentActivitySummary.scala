package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._

class TestLearnerContentActivitySummary extends SparkSpec(null) {

    "LearnerContentActivitySummary" should " write activities into learneractivity table and check the fields" in {

        val learnerContentAct = LearnerContentActivity("test-user-123", "test_content", 0.0d, 1, 1);
        val rdd = sc.parallelize(Array(learnerContentAct));
        rdd.saveToCassandra("learner_db", "learnercontentsummary");

        val rdd1 = loadFile[MeasuredEvent]("src/test/resources/learner-content-summary/learner_content_test_sample.log");
        LearnerContentActivitySummary.execute(sc, rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerContentActivitySummary")));
        val rowRDD = sc.cassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary");
        val learnerContent = rowRDD.map { x => ((x.learner_id), (x.content_id, x.interactions_per_min, x.num_of_sessions_played, x.time_spent)) }.toArray().toMap;

        val user1 = learnerContent.get("test-user-123").get;
        val user2 = learnerContent.get("5704ec89-f6e3-4708-9833-ddf7c57b3949").get;

        user1._2 should be(1)
        user1._3 should be(1)
        user1._4 should be(0)

        user2._2 should be(2.81)
        user2._3 should be(3)
        user2._4 should be(1092)
    }
    
//    it should " store learner content summary data into db from the given file " in {
//        val rdd1 = loadFile[MeasuredEvent]("src/test/resources/learner-content-summary/session-summary-prod-test.log");
//        LearnerContentActivitySummary.execute(sc, rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerContentActivitySummary")));
//    }
}