package org.ekstep.analytics.updater

import org.scalatest._
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
class TestUpdateLearnerActivity extends SparkSpec(null) {
  
    "UpdateLearnerActivity" should "parse learner activity summary and populater learner snapshot" in {
        
        val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-snapshot-updater/la-events.log"))))));
        val rdd2 = UpdateLearnerActivity.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be (1);
        out(0) should be ("Learner database updated sucessfully");
    }
    
}