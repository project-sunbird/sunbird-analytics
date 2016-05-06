package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateLearnerActivity
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.IJob

/**
 * @author Santhosh
 */

@Deprecated
object LearnerSnapshotUpdater extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.LearnerSnapshotUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.info("Started executing LearnerSnapshotUpdater Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MeasuredEvent]("batch", config, UpdateLearnerActivity);
        JobLogger.info("LearnerSnapshotUpdater Job completed", className)
    }

}