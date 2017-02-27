package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.job.consolidated._
import org.ekstep.analytics.views.PrecomputedViewsJob

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
            case "influxdb-updater" =>
                InfluxDBUpdater    
            case _ =>
                throw new JobNotFoundException("Unknown job type found");
        }
    }
}