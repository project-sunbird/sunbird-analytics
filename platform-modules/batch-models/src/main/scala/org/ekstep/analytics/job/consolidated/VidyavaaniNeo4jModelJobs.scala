package org.ekstep.analytics.job.consolidated

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.vidyavaani.job.PopulateOwnerNodeJob

object VidyavaaniNeo4jModelJobs extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.consolidated.VidyavaaniNeo4jModelJobs";

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        val jobs = List(PopulateOwnerNodeJob)
        for (job <- jobs) {
            job.main(config)
        }
    }
}