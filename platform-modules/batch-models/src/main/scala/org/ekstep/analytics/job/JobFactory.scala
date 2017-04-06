package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.job.consolidated._
import org.ekstep.analytics.views.PrecomputedViewsJob
import org.ekstep.analytics.vidyavaani.job.ContentLanguageRelationModel
import org.ekstep.analytics.vidyavaani.job.ContentAssetRelationModel
import org.ekstep.analytics.updater.CreationMetricsUpdater
import org.ekstep.analytics.vidyavaani.job.AuthorRelationsModel
import org.ekstep.analytics.vidyavaani.job.ConceptLanguageRelationModel
import org.ekstep.analytics.model.EOCRecommendationFunnelModel
import org.ekstep.analytics.vidyavaani.job.CreationRecommendationEnrichmentModel
import org.ekstep.analytics.vidyavaani.job.CreationRecommendationModel

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
                CreationMetricsModelUpdater
            case "eocfs" =>
                EOCRecommendationFunnelSummarizer
            case "creation-reco-enrichment" =>
                CreationRecommendationEnrichmentModel
            case "creation-reco" =>
                CreationRecommendationModel
            case _ =>
                throw new JobNotFoundException("Unknown job type found");
        }
    }
}