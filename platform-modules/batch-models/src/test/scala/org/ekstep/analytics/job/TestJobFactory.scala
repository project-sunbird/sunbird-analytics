package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.exception.JobNotFoundException

class TestJobFactory extends BaseSpec {

    it should "return a Model class for a model code" in {

        val jobIds = List("monitor-job-summ", "wfs", "wfus", "wfu", "ds", "dpu", "video-streaming", "etb-coverage-summary", "portal-metrics", "workflow-usage-metrics", "data-exhaust", "course-dashboard-metrics", "telemetry-replay", "summary-replay", "content-rating-updater", "experiment", "daily-metrics-consumption-reports", "assessment-dashboard-metrics", "druid-query-processor", "admin-user-reports")

        val jobs = jobIds.map { f => JobFactory.getJob(f) }

        jobs(1) should be(WorkFlowSummarizer)
        jobs(1).isInstanceOf[IJob] should be(true)

        jobs(10) should be(DataExhaustJob)
        jobs(10).isInstanceOf[IJob] should be(true)

        jobs(3) should be(WorkFlowUsageUpdater)
        jobs(3).isInstanceOf[IJob] should be(true)
    }

    it should "return JobNotFoundException" in {

        the[JobNotFoundException] thrownBy {
            JobFactory.getJob("test-model")
        } should have message "Unknown job type found"
    }

}