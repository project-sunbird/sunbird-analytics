package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Filter

class TestProficiencyUpdaterJob extends BaseSpec {
    
    "ProficiencyUpdaterJob" should "execute the job without throwing exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-proficiency/proficiency_update_db_test1.log"))))), None, None, "org.ekstep.analytics.model.ProficiencyUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestProficiencyUpdaterJob"), Option(false))
        ProficiencyUpdater.main(JSONUtils.serialize(config));
    }
    
    ignore should "execute the job on production data" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("/Users/Santhosh/ekStep/telemetry_dump/prod.analytics.screener-2016-01-28-06-39.json.gz"))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.ProficiencyUpdater", Option(Map("alpha" -> 1.0d.asInstanceOf[AnyRef], "beta" -> 1.0d.asInstanceOf[AnyRef])), Option(Array(Dispatcher("file", Map("file" -> "prof_output.log")))), Option(10), Option("TestProficiencyUpdaterJob"), Option(false))
        ProficiencyUpdater.main(JSONUtils.serialize(config));
    }
}