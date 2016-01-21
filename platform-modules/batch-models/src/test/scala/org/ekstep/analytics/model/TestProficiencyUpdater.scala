package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent

class TestProficiencyInputMapper extends SparkSpec(null) {
    
    it should "print the item data for testing" in {
        val prof = new ProficiencyUpdater();
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/test.log");
        val rdd2 = prof.execute(sc, rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerProficiencyUpdater")));
        var out = rdd2.collect();
        for (e <- out) {
            println(e)
        }
    }
}