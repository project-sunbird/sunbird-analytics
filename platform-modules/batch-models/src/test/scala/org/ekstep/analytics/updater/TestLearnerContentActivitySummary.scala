package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._
import java.util.UUID
import org.ekstep.analytics.framework.LearnerId

class TestLearnerContentActivitySummary extends SparkSpec(null) {

    "LearnerContentActivitySummary" should " write activities into learneractivity table and check the fields" in {

        val learnerContentAct = LearnerContentActivity("8b4f3775-6f65-4abf-9afa-b15b8f82a24b", "test_content", 0.0d, 1, 1);
        val rdd = sc.parallelize(Array(learnerContentAct));
        rdd.saveToCassandra("learner_db", "learnercontentsummary");

        val rdd1 = loadFile[MeasuredEvent]("src/test/resources/learner-content-summary/learner_content_test_sample.log");
        LearnerContentActivitySummary.execute(sc, rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerContentActivitySummary")));
        val rowRDD = sc.cassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary");
        rowRDD.count() should be(3);
        val learnerContent = rowRDD.map { x => ((x.learner_id), (x.content_id, x.interactions_per_min, x.num_of_sessions_played, x.time_spent)) }.toArray().toMap;

        val user1 = learnerContent.get("4320ab1a-7789-453d-910c-8d5a3e3ea52b").get;
        val user2 = learnerContent.get("5704ec89-f6e3-4708-9833-ddf7c57b3949").get;

        user1._2 should be(0)
        user1._3 should be(1)
        user1._4 should be(4)

        user2._2 should be(2.81)
        user2._3 should be(3)
        user2._4 should be(1092)
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