package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.ProfileEvent
import com.datastax.spark.connector._

/**
 * @author Santhosh
 */
class TestLearnerProfileUpdater extends SparkSpec(null) {
    
    "LearnerProfileUpdater" should "update learner profile in learner db" in {

        val rdd = loadFile[ProfileEvent]("src/test/resources/learner-profile/2016-04-04-1459753547783.json");
        UpdateLearnerProfileDB.execute(rdd, None);
        
        val profile1 = sc.cassandraTable[LearnerProfile]("learner_db", "learnerprofile").where("learner_id = ?", "18e2152f-b1a4-45b4-94f1-091ca1d6de9e").first();
        profile1.age should be (-1);
        profile1.anonymous_user should be (false);
        profile1.did should be ("d5ef0395fb76e056d54758007ae353f16d898a7b");
        profile1.gender should be (None);
        profile1.group_user should be (true);
        profile1.language.get should be ("en");
        profile1.standard should be (-1);
        profile1.year_of_birth should be (-1);
        profile1.updated_date should not be null;
        
        val profile2 = sc.cassandraTable[LearnerProfile]("learner_db", "learnerprofile").where("learner_id = ?", "1aca2342-3865-4f67-aff5-048027cba8b1").first();
        profile2.age should be (7);
        profile2.anonymous_user should be (false);
        profile2.did should be ("d5ef0395fb76e056d54758007ae353f16d898a7b");
        profile2.gender.get should be ("female");
        profile2.group_user should be (false);
        profile2.language.get should be ("hi");
        profile2.standard should be (7);
        profile2.year_of_birth should be (2009);
        profile2.updated_date should not be null;
        
        val profile3 = sc.cassandraTable[LearnerProfile]("learner_db", "learnerprofile").where("learner_id = ?", "f770ee2c-2e2a-48fc-890b-8634718ae003").first();
        profile3.age should be (-1);
        profile3.anonymous_user should be (true);
        profile3.did should be ("d5ef0395fb76e056d54758007ae353f16d898a7b");
        profile3.gender should be (None);
        profile3.group_user should be (false);
        profile3.language should be (None);
        profile3.standard should be (-1);
        profile3.year_of_birth should be (-1);
        profile3.updated_date should not be null;
    }
  
}