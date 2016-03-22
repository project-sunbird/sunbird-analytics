package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.BaseSpec

class TestRePlayModelSupervisor extends BaseSpec {

    ignore should " Run LP from S3 " in {
        val config = JobConfig(Fetcher("s3", None,  Option(Array(Query(Option("ekstep-session-summary"), Option("prod.analytics.screener-"), Option("2016-02-21"), Option("2016-02-23"))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.ProficiencyUpdater", Option(Map("alpha" -> 1.0d.asInstanceOf[AnyRef], "beta" -> 1.0d.asInstanceOf[AnyRef])), Option(Array(Dispatcher("file", Map("file" -> "prof_output.log")))), Option(10), Option("TestModelSupervisor"), Option(false))
        RePlayModelSupervisor.main("LearnerProficiencySummary", "2016-02-22", "2016-02-23", JSONUtils.serialize(config));
    }
    it should " Run LP from local file" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-proficiency/proficiency_update_db_test1.log"))))), None, None, "org.ekstep.analytics.model.ProficiencyUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestModelSupervisor"), Option(false))
        println(JSONUtils.serialize(config))
        RePlayModelSupervisor.main("LearnerProficiencySummary", "2016-02-22", "2016-02-23", JSONUtils.serialize(config));
    }
}