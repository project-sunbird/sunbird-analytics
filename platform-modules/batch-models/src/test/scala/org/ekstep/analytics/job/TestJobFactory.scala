package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.framework.IJob

class TestJobFactory extends BaseSpec {

    it should "return a Model class for a model code" in {

        val jobIds = List("wfs", "wfus", "wfu", "data-exhaust")

        val jobs = jobIds.map { f => JobFactory.getJob(f) }

        jobs(0) should be(WorkFlowSummarizer)
        jobs(0).isInstanceOf[IJob] should be(true)

        jobs(3) should be(DataExhaustJob)
        jobs(3).isInstanceOf[IJob] should be(true)

        jobs(2) should be(WorkFlowUsageUpdater)
        jobs(2).isInstanceOf[IJob] should be(true)
    }

}