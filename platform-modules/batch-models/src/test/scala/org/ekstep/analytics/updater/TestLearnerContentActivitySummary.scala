package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._
import java.util.UUID
import org.ekstep.analytics.framework.LearnerId

class TestLearnerContentActivitySummary extends SparkSpec(null) {

    "LearnerContentActivitySummary" should " write activities into learneractivity table and check the fields" in {

        val learnerContentAct = LearnerContentActivity("test-user-123", "test_content", 0.0d, 1, 1);
        val rdd = sc.parallelize(Array(learnerContentAct));
        rdd.saveToCassandra("learner_db", "learnercontentsummary");

        val rdd1 = loadFile[MeasuredEvent]("src/test/resources/learner-content-summary/learner_content_test_sample.log");
        LearnerContentActivitySummary.execute(sc, rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerContentActivitySummary")));
        val rowRDD = sc.cassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary");
        val learnerContent = rowRDD.map { x => ((x.learner_id), (x.content_id, x.interactions_per_min, x.num_of_sessions_played, x.time_spent)) }.collect.toMap;

        val user1 = learnerContent.get("test-user-123").get;

        user1._2 should be(1)
        user1._3 should be(1)
        user1._4 should be(0)
        
    }
    
    /*
    it should "test join with learner ids" in {
        val learner_id = "32b8e28b-9945-4264-9bc5-3c8ce087231c";
        
        val lcs = Array(LearnerContentActivity(learner_id, "Content1", 0.0d, 1, 1),
                LearnerContentActivity(learner_id, "Content2", 0.0d, 1, 1),
                LearnerContentActivity(learner_id, "Content3", 0.0d, 1, 1));
        val rdd = sc.parallelize(lcs);
        rdd.saveToCassandra("learner_db", "learnercontentsummary");
        
        val rdd2 = sc.parallelize(Array(learner_id), 1);
        val result = rdd2.map { x => LearnerId(x) }.joinWithCassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary");
        result.groupBy(f => f._1).collect().foreach { x => 
            println("learner_id", x._1);
            println("content activity summaries size", x._2.size);
        }
    }*/
}