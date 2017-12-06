package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.LearnerProfile
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.ProfileEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestUpdateLearnerProfileUpdateDB extends SparkSpec(null) {

    "UpdateLearnerProfileUpdateDB" should "update learner profile in learner db" in {
        val rdd = loadFile[ProfileEvent]("src/test/resources/learner-profile/test-data2.log");
        UpdateLearnerProfileUpdateDB.execute(rdd, None);

        val profile1 = sc.cassandraTable[LearnerProfile]("local_learner_db", "learnerprofile").where("learner_id = ?", "e1a8e04a-0aa5-4214-8dfb-d436ba23117b").first();

        profile1.age should be(7);
        profile1.anonymous_user should be(false);
        profile1.did should be("159e749b9cc0aa46d92419f2b54fc400a8888bc6");
        profile1.gender.get should be("female");
        profile1.group_user should be(false);
        profile1.language.get should be("en");
        profile1.standard should be(2);
        profile1.year_of_birth should be(2010);
        profile1.updated_date should not be null;

        val profile2 = sc.cassandraTable[LearnerProfile]("local_learner_db", "learnerprofile").where("learner_id = ?", "dec99d48-e258-4b4a-9966-d14bda0c7f10").first();
        
        profile2.age should be(5);
        profile2.anonymous_user should be(false);
        profile2.did should be("159e749b9cc0aa46d92419f2b54fc400a8888b100");
        profile2.gender.get should be("male");
        profile2.group_user should be(false);
        profile2.language.get should be("en");
        profile2.standard should be(2);
        profile2.year_of_birth should be(2012);
        profile2.updated_date should not be null;
    }

}