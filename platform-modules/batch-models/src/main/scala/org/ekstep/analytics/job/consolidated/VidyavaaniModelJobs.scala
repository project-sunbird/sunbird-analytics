package org.ekstep.analytics.job.consolidated

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.vidyavaani.job.ContentOwnerRelationModel
import org.ekstep.analytics.vidyavaani.job.ContentLanguageRelationModel
import org.ekstep.analytics.vidyavaani.job.ContentAssetRelationModel

object VidyavaaniModelJobs extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.consolidated.VidyavaaniNeo4jModelJobs";

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        val jobs = List(ContentOwnerRelationModel, ContentLanguageRelationModel, ContentAssetRelationModel)
        for (job <- jobs) {
            job.main(config)
        }
    }
}