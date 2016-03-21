package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateLearnerActivity
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext

/**
 * @author Santhosh
 */

@Deprecated
object LearnerSnapshotUpdater extends optional.Application {

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MeasuredEvent]("batch", config, UpdateLearnerActivity);
    }

}