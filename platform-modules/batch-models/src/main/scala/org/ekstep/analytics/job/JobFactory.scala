package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.job.consolidated._
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.views.PrecomputedViewsJob

/**
 * @author Santhosh
 */

object JobFactory {
    @throws(classOf[JobNotFoundException])
    def getJob(jobType: String): IJob = {
        jobType.toLowerCase() match {
            case "as" =>
                AserScreenSummarizer
            case "ss" =>
                LearnerSessionSummarizer
            case "las" =>
                LearnerActivitySummarizer
            case "lp" =>
                ProficiencyUpdater
            case "ls" =>
                LearnerSnapshotUpdater
            case "lcas" =>
                LearnerContentActivityUpdater
            case "lcr" =>
                RecommendationEngineJob
            case "cus" =>
                ContentUsageSummarizer
            case "cps" =>
                ContentPopularitySummarizer
            case "cpu" =>
                ContentPopularityUpdater
            case "cuu" =>
                ContentUsageUpdater
            case "gss" =>
                GenieUsageSessionSummarizer
            case "gls" =>
                GenieLaunchSummarizer
            case "gls-v1" =>
                GenieLaunchV1Summarizer
            case "dus" =>
                DeviceUsageSummarizer
            case "css" =>
                ContentSideloadingSummarizer
            case "lpu" =>
                LearnerProfileUpdater
            case "lpc" =>
                LearnerProfileCreater
            case "cmu" =>
                ContentModelUpdater
            case "dsu" =>
                DeviceSpecificationUpdater
            case "is" =>
                ItemSummarizer
            case "dcus" =>
                DeviceContentUsageSummarizer
            case "csv" =>
                CSVDumpJob
            case "ctv" =>
                ContentToVecJob
            case "device-recos-training" =>
                DeviceRecommendationTrainingJob
            case "device-recos-scoring" =>
                DeviceRecommendationScoringJob
            case "raw-telemetry-jobs" =>
                RawTelemetryJobs
            case "ss-jobs" =>
                SessionSummaryJobs
            case "raw-telemetry-updaters" =>
                RawTelemetryUpdaters
            case "sts" =>
                StageSummarizer
            case "genie-launch-summ" =>
                GenieUsageSummarizer
            case "genie-launch-metrics" =>
                GenieUsageUpdater
            case "item-usage-summ" =>
                ItemUsageSummarizer
            case "item-usage-metrics" =>
                ItemSummaryUpdater
            case "gsts" =>
                GenieStageSummarizer
            case "gfs" =>
                GenieFunnelSummarizer
            case "gfa" =>
                GenieFunnelAggregatorJob
            case "data-exhaust" =>
                DataExhaustJob
            case "precomp-views" =>
                PrecomputedViewsJob
            case "content-recos" =>
                EndOfContentRecommendationJob
            case "content-own-rel" =>
                AuthorRelationsModel
            case "content-lang-rel" =>
                ContentLanguageRelationModel
            case "content-asset-rel" =>
                ContentAssetRelationModel
            case "concept-lan-rel" =>
                ConceptLanguageRelationModel
            case "vidyavaani-jobs" =>
                VidyavaaniModelJobs
            case "consumption-metrics" =>
                ConsumptionMetricsUpdater
            case "creation-metrics" =>
                CreationMetricsUpdater
            case "eocfs" =>
                EOCRecommendationFunnelSummarizer
            case "re-enrichment-model" =>
                CreationRecommendationEnrichmentModel
            case "creation-reco" =>
                CreationRecommendationModel
            case "content-snapshot" =>
                ContentSnapshotSummarizer
            case "concept-snapshot" =>
                ConceptSnapshotSummarizer
            case "asset-snapshot" =>
                AssetSnapshotSummarizer
            case "asset-snapshot-updater" =>
                AssetSnapshotUpdater
            case "content-snapshot-updater" =>
                ContentSnapshotUpdater
            case "concept-snapshot-updater" =>
                ConceptSnapshotUpdater
            case "ce-ss" =>
                ContentEditorSessionSummarizer
            case "ce-usage" =>
                ContentEditorUsageSummarizer
            case "ce-usage-updater" =>
                ContentEditorUsageUpdater
            case "content-creation-metrics" =>
                ContentCreationMetricsUpdater
            case "textbook-snapshot-updater" =>
                TextbookSnapshotUpdater
            case "app-ss" =>
                AppSessionSummarizer
            case "app-ss-v1" =>
                AppSessionV1Summarizer
            case "obj-cache-updater" =>
                AppObjectCacheUpdater
            case "textbook-ss" =>
                TextbookSessionSummarizer
            case "textbook-usage" =>
                TextbookUsageSummarizer
            case "textbook-usage-updater" =>
                TextbookUsageUpdater
            case "author-usage-summary" =>
                AuthorUsageSummarizer
            case "author-usage-updater" =>
                AuthorSummaryUpdater
            case "app-usage" =>
                AppUsageSummarizer
            case "app-usage-updater" =>
                AppUsageUpdater
            case "publish-pipeline-summ" =>
                PublishPipelineSummarizer
            case "app-raw-telemetry-jobs" =>
                AppRawTelemetryJobs
            case "app-raw-telemetry-updaters" =>
                AppRawTelemetryUpdaters
            case "publish-pipeline-updater" =>
                PublishPipelineUpdater
            case "me-metrics" =>
                MetricsEventCreationJob
            case "monitor-job-summ" =>
                MonitorSummarizer
            case "plugin-snapshot-updater" =>
                PluginSnapshotMetricsUpdater
            case "api-usage-summ" =>
                APIUsageSummarizer
            case "template-snapshot-updater" =>
                TemplateSnapshotMetricsUpdater
            case "usage-summary" =>
                UsageSummarizer
            case "usage-updater" =>
                UsageUpdater
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
            case "assessment-dashboard-metric" =>
                AssessmentMetricsJob
            case "druid-query-processor" =>
                DruidQueryProcessor
            case _ =>
                throw new JobNotFoundException("Unknown job type found")
        }
    }
}