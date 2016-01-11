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

    def main(config: String, cc: Option[Boolean]) {
        JobDriver.run[MeasuredEvent]("batch", config, UpdateLearnerActivity);
        if(cc.nonEmpty && cc.get) {
            LearnerAdapter.session.close();
            LearnerAdapter.session.getCluster.close();            
        }
    }

}