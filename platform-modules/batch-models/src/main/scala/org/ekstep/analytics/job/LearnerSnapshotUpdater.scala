package org.ekstep.analytics.job

import org.ekstep.analytics.framework.util.Application
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateLearnerActivity
import org.ekstep.analytics.framework.MeasuredEvent

/**
 * @author Santhosh
 */
object LearnerSnapshotUpdater extends Application {

    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, UpdateLearnerActivity);
    }

}