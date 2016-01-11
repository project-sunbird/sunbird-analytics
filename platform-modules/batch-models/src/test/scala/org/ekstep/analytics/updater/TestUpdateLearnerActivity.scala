package org.ekstep.analytics.updater

import org.scalatest._
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.adapter.learner.LearnerAdapter
import org.ekstep.analytics.adapter.learner.Defaults

/**
 * @author Santhosh
 */
class TestUpdateLearnerActivity extends SparkSpec {
  
    "UpdateLearnerActivity" should "parse learner activity summary and populater learner snapshot" in {
        
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-snapshot-updater/la-events.log")
        val rdd2 = UpdateLearnerActivity.execute(sc, rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be (2);
        out(0) should be (out(1));
    }
}