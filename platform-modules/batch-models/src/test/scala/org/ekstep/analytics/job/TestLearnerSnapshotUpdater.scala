package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestLearnerSnapshotUpdater extends BaseSpec {
    
    ignore should "execute LearnerSnapshotUpdater job and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-activity-summary/learner_activity_summary_sample1.log"))))), None, None, "org.ekstep.analytics.updater.UpdateLearnerActivity", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerSnapshotUpdater"), Option(false))
        LearnerSnapshotUpdater.main(JSONUtils.serialize(config), None);
    }
}