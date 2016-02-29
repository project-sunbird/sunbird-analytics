package org.ekstep.analytics.job

import optional.Application
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateLearnerActivity
import org.ekstep.analytics.framework.MeasuredEvent

/**
 * @author Santhosh
 */

@Deprecated
object LearnerSnapshotUpdater extends Application {

    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, UpdateLearnerActivity);
    }

}