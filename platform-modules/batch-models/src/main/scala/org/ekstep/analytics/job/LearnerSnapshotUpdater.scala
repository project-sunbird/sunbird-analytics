package org.ekstep.analytics.job

import optional.Application
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateLearnerActivity
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.adapter.learner.LearnerAdapter

/**
 * @author Santhosh
 */
object LearnerSnapshotUpdater extends Application {

    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, UpdateLearnerActivity);
        LearnerAdapter.session.close();
        LearnerAdapter.session.getCluster.close();
    }

}