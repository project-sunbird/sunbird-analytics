package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.job.updater.ProficiencyUpdater

class TestProficiencyUpdaterJob extends SparkSpec(null) {
    
    "ProficiencyUpdaterJob" should "execute the job without throwing exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-proficiency/proficiency_update_db_test1.log"))))), None, None, "org.ekstep.analytics.model.ProficiencyUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestProficiencyUpdaterJob"), Option(false))
        ProficiencyUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
    
    ignore should "execute the job on production data" in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("sandbox-session-summary"), Option("sandbox.analytics.screener-"), None, Option("2016-02-10"), Option(0))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.ProficiencyUpdater", Option(Map("alpha" -> 1.0d.asInstanceOf[AnyRef], "beta" -> 1.0d.asInstanceOf[AnyRef])), Option(Array(Dispatcher("file", Map("file" -> "prof_output.log")))), Option(10), Option("TestProficiencyUpdaterJob"), Option(false))
        ProficiencyUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}