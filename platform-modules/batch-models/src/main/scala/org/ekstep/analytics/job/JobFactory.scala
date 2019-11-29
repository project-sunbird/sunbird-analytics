package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.job.report.CourseMetricsJob
import org.ekstep.analytics.job.report.StateAdminReportJob
import org.ekstep.analytics.job.report.AssessmentMetricsJob
import org.ekstep.analytics.job.batch.VideoStreamingJob

/**
 * @author Santhosh
 */

object JobFactory {
    @throws(classOf[JobNotFoundException])
    def getJob(jobType: String): IJob = {
        jobType.toLowerCase() match {
            case "monitor-job-summ" =>
                MonitorSummarizer
            case "wfs" =>
                WorkFlowSummarizer
            case "wfus" =>
                WorkFlowUsageSummarizer
            case "wfu" =>
                WorkFlowUsageUpdater
            case "ds" =>
                DeviceSummarizer
            case "dpu" =>
                DeviceProfileUpdater
            case "video-streaming" =>
                VideoStreamingJob
            case "portal-metrics" =>
                PortalMetricsUpdater
            case "workflow-usage-metrics" =>
                WorkFlowUsageMetricsUpdater
            case "data-exhaust" =>
                DataExhaustJob
            case "course-dashboard-metrics" =>
                CourseMetricsJob
            case "telemetry-replay" =>
                EventsReplayJob
            case "summary-replay" =>
                EventsReplayJob
            case "content-rating-updater" =>
                ContentRatingUpdater
            case "experiment" =>
                ExperimentDefinitionJob
            case "assessment-dashboard-metrics" =>
                AssessmentMetricsJob
            case "druid-query-processor" =>
                DruidQueryProcessor
            case "admin-user-reports" =>
                StateAdminReportJob
            case _ =>
                throw new JobNotFoundException("Unknown job type found")
        }
    }
}