package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestFixSecorValidBackupJob extends SparkSpec(null) {
  
    ignore should "execute FixSecorValidBackupJob job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("S3", None, Option(Array(Query(Option("ekstep-dev-data-store"), Option("valid/"), None, Option("2017-12-23"), Option(0), None, None, None, None, None)))), None, null, "org.ekstep.analytics.model.SecorValidBackupFix", None, None, Option(10), Option("TestSecorValidBackupFix"), Option(false))
        FixSecorValidBackupJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}