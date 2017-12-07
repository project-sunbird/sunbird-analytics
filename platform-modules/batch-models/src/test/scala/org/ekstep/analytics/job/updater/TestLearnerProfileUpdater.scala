package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestLearnerProfileUpdater extends SparkSpec(null) {

    "LearnerProfileUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-profile/test-data2.log"))))), None, None, "org.ekstep.analytics.job.updater.LearnerProfileUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerProfileUpdaterJob"), Option(false))
        LearnerProfileUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}