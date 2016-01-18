package org.ekstep.analytics.updater

import org.scalatest._
import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.LearnerSnapshotUpdater
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
class TestUpdateLearnerActivity extends BaseSpec {
  
    "UpdateLearnerActivity" should "parse learner activity summary and populater learner snapshot" in {
        
        val sc = CommonUtil.getSparkContext(1, "Test");
        val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-snapshot-updater/la-events.log"))))));
        val rdd2 = UpdateLearnerActivity.execute(sc, rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be (1);
        out(0) should be ("Learner database updated sucessfully");
        CommonUtil.closeSparkContext(sc);
    }
    
    "LearnerSnapshotUpdater" should "execute LearnerSnapshotUpdater job and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-activity-summary/learner_activity_summary_sample1.log"))))), None, None, "org.ekstep.analytics.updater.UpdateLearnerActivity", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerSnapshotUpdater"), Option(false))
        LearnerSnapshotUpdater.main(JSONUtils.serialize(config));
    }
}