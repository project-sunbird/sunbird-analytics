package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._

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
            case "dialcode-usage-summary" =>
                DialcodeUsageSummarizer
            case "etb-coverage-summary" =>
                ETBCoverageSummarizer
            case "portal-metrics" =>
                PortalMetricsUpdater
            case "workflow-usage-metrics" =>
                WorkFlowUsageMetricsUpdater
            case "dialcode-usage-updater" =>
                DialcodeUsageUpdater
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
            case "daily-metrics-consumption-reports" =>
                ConsumptionReportsJob
            case "assessment-dashboard-metrics" =>
                AssessmentMetricsJob
            case "druid-query-processor" =>
                DruidQueryProcessor
            case "pipeline-failed-events-audit" =>
                AuditComputationJob
            case "pipeline-druid-events-audit" =>
                AuditComputationJob
            case "pipeline-audit" =>
                AuditComputationJob
            case "admin-user-reports" =>
                StateAdminReportJob
            case _ =>
                throw new JobNotFoundException("Unknown job type found")
        }
    }
}