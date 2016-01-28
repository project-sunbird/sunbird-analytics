package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.JSONUtils

class TestProficiencyUpdater extends SparkSpec(null) {

    "ProficiencyUpdater" should "check Proficiency and Model param should be updated in db" in {
        
        val modelParams = Map("alpha" -> 1, "beta" -> 1);
        // Override user data in learnerproficiency table
        val learnerProf = LearnerProficiency("8b4f3775-6f65-4abf-9afa-b15b8f82a24b", Map("Num:C3:SC1:MC12" -> 0.5), DateTime.now(), DateTime.now(), Map("Num:C3:SC1:MC12" -> JSONUtils.serialize(modelParams)));
        val rdd = sc.parallelize(Array(learnerProf));
        rdd.saveToCassandra("learner_db", "learnerproficiency");
        
        val rdd0 = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/proficiency_update_db_test1.log");
        val rdd01 = ProficiencyUpdater.execute(sc, rdd0, Option(Map("modelVersion" -> "1.0", "modelId" -> "ProficiencyUpdater")));
        val proficiency1 = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").where("learner_id = ?", "8b4f3775-6f65-4abf-9afa-b15b8f82a24b").first();
        
        println("proficiency1", proficiency1);
        // Check Proficiency and Model parameter values - Iteration 1
        proficiency1.model_params.contains("Num:C3:SC1:MC12") should be(true);
        val modelParams1 = JSONUtils.deserialize[Map[String,Double]](proficiency1.model_params.get("Num:C3:SC1:MC12").get);
        modelParams1.get("alpha").get should be (4.0);
        modelParams1.get("beta").get should be (2.0);
        
        proficiency1.proficiency.contains("Num:C3:SC1:MC12") should be(true);
        proficiency1.proficiency.get("Num:C3:SC1:MC12").get should be (0.67);

        val rdd1 = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/proficiency_update_db_test2.log");
        val rdd11 = ProficiencyUpdater.execute(sc, rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "ProficiencyUpdater")));

        // Check Proficiency and Model parameter values - Iteration 2
        val proficiency2 = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").where("learner_id = ?", "8b4f3775-6f65-4abf-9afa-b15b8f82a24b").first();
        println("proficiency2", proficiency2);
        proficiency2.model_params.contains("Num:C3:SC1:MC12") should be(true)
        proficiency2.model_params.contains("Num:C3:SC1:MC13") should be(true)
        val modelParams2 = JSONUtils.deserialize[Map[String,Double]](proficiency2.model_params.get("Num:C3:SC1:MC12").get);
        modelParams2.get("alpha").get should be (6);
        modelParams2.get("beta").get should be (2);
        
        val modelParams3 = JSONUtils.deserialize[Map[String,Double]](proficiency2.model_params.get("Num:C3:SC1:MC13").get);
        modelParams3.get("alpha").get should be (2.5);
        modelParams3.get("beta").get should be (1);
        
        proficiency2.proficiency.contains("Num:C3:SC1:MC12") should be(true)
        proficiency2.proficiency.get("Num:C3:SC1:MC12").get should be (0.75);
        
        proficiency2.proficiency.contains("Num:C3:SC1:MC13") should be(true)
        proficiency2.proficiency.get("Num:C3:SC1:MC13").get should be (0.71);

        var out = rdd01.collect();
        out.length should be(1)

        var out1 = rdd11.collect();
        out1.length should be(1)
    }

    "it" should "print the item data for testing" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/test.log");
        val rdd2 = ProficiencyUpdater.execute(sc, rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "ProficiencyUpdater")));
        var out = rdd2.collect();
        out.length should be (10)
    }
    
    it should "check the zero Proficiency Updater event is coming" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/emptyMC_test.log");
        val rdd2 = ProficiencyUpdater.execute(sc, rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "ProficiencyUpdater")));
        var out = rdd2.collect();
        out.length should be(2)
    }
}