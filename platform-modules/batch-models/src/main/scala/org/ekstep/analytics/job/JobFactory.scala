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
                AserScreenSummarizer;
            case "ss" =>
                LearnerSessionSummarizer;
            case "las" =>
                LearnerActivitySummarizer;
            case "lp" =>
                ProficiencyUpdater;
            case "ls" =>
                LearnerSnapshotUpdater;
            case "lcas" =>
                LearnerContentActivityUpdater;
            case "lcr" =>
                RecommendationEngineJob;
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
            case "dus" =>
                DeviceUsageSummarizer
            case "css" =>
                ContentSideloadingSummarizer
            case "lpu" =>
                LearnerProfileUpdater
            case "cmu" =>
                ContentModelUpdater
            case "dsu" =>
                DeviceSpecificationUpdater
            case "is" =>
                ItemUsageSummarizer
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
                ItemSummarizer
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
            case "ce-us" =>
                ContentEditorUsageSummarizer
            case "content-creation-metrics" =>
                ContentCreationMetricsUpdater
            case "textbook-snapshot-updater" =>
            	TextbookSnapshotUpdater;
            case "app-ss" =>
            	PortalSessionSummarizer;
            case "obj-cache-updater" =>
            	AppObjectCacheUpdater
            case "portal-usage" =>
            	PortalUsageSummarizer;
            case _ =>
                throw new JobNotFoundException("Unknown job type found");
        }
    }
}