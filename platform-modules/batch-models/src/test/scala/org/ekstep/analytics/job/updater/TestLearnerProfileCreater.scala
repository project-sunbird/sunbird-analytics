package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestLearnerProfileCreater extends SparkSpec(null) {

    "LearnerProfileCreater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-profile/test-data1.log"))))), None, None, "org.ekstep.analytics.job.updater.LearnerProfileCreater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerProfileUpdaterJob"), Option(false))
        LearnerProfileCreater.main(JSONUtils.serialize(config))(Option(sc));
    }
}