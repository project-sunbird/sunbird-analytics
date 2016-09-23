package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.updater.UpdateLearnerActivityDB

/**
 * @author Santhosh
 */

@Deprecated
object LearnerSnapshotUpdater extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.LearnerSnapshotUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, UpdateLearnerActivityDB);
        JobLogger.log("Job Completed.")
    }

}