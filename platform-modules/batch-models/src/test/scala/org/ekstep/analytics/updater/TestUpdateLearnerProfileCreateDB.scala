package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.V3ProfileEvent
import org.ekstep.analytics.framework.LearnerProfile
import org.ekstep.analytics.framework.util.JSONUtils

class TestUpdateLearnerProfileCreateDB extends SparkSpec(null) {
    
    "UpdateLearnerProfileCreateDB" should "update learner profile in learner db for create User" in {

        val rdd = loadFile[V3ProfileEvent]("src/test/resources/learner-profile/test-data1.log");
        UpdateLearnerProfileCreateDB.execute(rdd, None);
        
        val profile1 = sc.cassandraTable[LearnerProfile]("local_learner_db", "learnerprofile").where("learner_id = ?", "287c1c6f-9c1e-437c-92da-279c5907becd").first();
        profile1.age should be (-1);
        profile1.anonymous_user should be (true);
        profile1.did should be ("d5ef0395fb76e056d54758007ae353f16d898a7b");
        profile1.gender should be (None);
        profile1.group_user should be (false);
        profile1.language should be (None);
        profile1.standard should be (-1);
        profile1.year_of_birth should be (-1);
        profile1.updated_date should not be null;
        
        val profile2 = sc.cassandraTable[LearnerProfile]("local_learner_db", "learnerprofile").where("learner_id = ?", "f770ee2c-2e2a-48fc-890b-8634718ae003").first();
        profile2.age should be (-1);
        profile2.anonymous_user should be (true);
        profile2.did should be ("d5ef0395fb76e056d54758007ae353f16d898a7b");
        profile2.gender should be (None);
        profile2.group_user should be (false);
        profile2.language should be (None);
        profile2.standard should be (-1);
        profile2.year_of_birth should be (-1);
        profile2.updated_date should not be null;
    }
    
    it should "ignore input with empty object.type" in {

        val rdd = loadFile[V3ProfileEvent]("src/test/resources/learner-profile/test-data3.log");
        UpdateLearnerProfileCreateDB.execute(rdd, None);
        
        val profile1 = sc.cassandraTable[LearnerProfile]("local_learner_db", "learnerprofile").where("learner_id = ?", "287").first();
        profile1.age should be (-1);
        profile1.anonymous_user should be (true);
        profile1.did should be ("d5ef0395fb76e056d54758007ae353f16d898a7b");
        profile1.gender should be (None);
        profile1.group_user should be (false);
        profile1.language should be (None);
        profile1.standard should be (-1);
        profile1.year_of_birth should be (-1);
        
        val profile2 = sc.cassandraTable[LearnerProfile]("local_learner_db", "learnerprofile").where("learner_id = ?", "770")
        profile2.count() should be(0)
    }
}