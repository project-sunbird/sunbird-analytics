package org.ekstep.analytics.job.consolidated

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.vidyavaani.job.ContentOwnerRelationModel
import org.ekstep.analytics.vidyavaani.job.ContentLanguageRelationModel
import org.ekstep.analytics.vidyavaani.job.ContentAssetRelationModel
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.CommonUtil

object VidyavaaniModelJobs extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.consolidated.VidyavaaniNeo4jModelJobs";

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.init("VidyavaaniModelJobs")
        JobLogger.start("VidyavaaniModelJobs Started executing", Option(Map("config" -> config)))

        val time = CommonUtil.time({
            val jobs = List(ContentOwnerRelationModel, ContentLanguageRelationModel, ContentAssetRelationModel)
            for (job <- jobs) {
                job.main(config)
            }
        })
        JobLogger.end("All VidyavaaniModelJobs Completed", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> time._1)));
    }
}