package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.model.SparkSpec

class TestProficiencyInputMapper extends SparkSpec(null){
  it should "print the item data for testing" in {
        val prof = new LearnerProficiencyMapper();
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/test.log");
        val rdd2 = prof.execute(sc, rdd, Option(Map()));
        var out = rdd2.collect();
        for(e<-out){
            println(e)
        }
    }
}