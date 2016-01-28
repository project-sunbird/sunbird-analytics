package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._

class TestProficiencyUpdater extends SparkSpec(null) {

    it should "check Proficiency and Model param should be updated in db" in {
        val rdd0 = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/proficiency_update_db_test1.log");
        val rdd01 = ProficiencyUpdater.execute(sc, rdd0, Option(Map("modelVersion" -> "1.0", "modelId" -> "ProficiencyUpdater")));
        val proficiency1 = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").first
        proficiency1.model_params.contains("Num:C3:SC1:MC12") should be(true)
        proficiency1.proficiency.contains("Num:C3:SC1:MC12") should be(true)

        val rdd1 = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/proficiency_update_db_test2.log");
        val rdd11 = ProficiencyUpdater.execute(sc, rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "ProficiencyUpdater")));

        val proficiency2 = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").first
        proficiency2.model_params.contains("Num:C3:SC1:MC12") should be(true)
        proficiency2.model_params.contains("Num:C3:SC1:MC13") should be(true)

        proficiency2.model_params.get("Num:C3:SC1:MC12").get should not be (proficiency1.model_params.get("Num:C3:SC1:MC12").get)
        
        proficiency2.proficiency.contains("Num:C3:SC1:MC12") should be(true)
        proficiency2.proficiency.contains("Num:C3:SC1:MC13") should be(true)
        
        proficiency2.proficiency.get("Num:C3:SC1:MC12").get should not be (proficiency1.proficiency.get("Num:C3:SC1:MC12").get)

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