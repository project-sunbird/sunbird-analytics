package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.summarizer.ContentUsageSummarizer
import org.ekstep.analytics.job.summarizer.DeviceUsageSummarizer
import org.ekstep.analytics.job.summarizer.LearnerActivitySummarizer
import org.ekstep.analytics.job.summarizer.AserScreenSummarizer
import org.ekstep.analytics.job.summarizer.ContentSideloadingSummarizer
import org.ekstep.analytics.job.summarizer.LearnerSessionSummarizer
import org.ekstep.analytics.job.summarizer.DeviceRecommendationTrainingJob
import org.ekstep.analytics.job.summarizer.GenieUsageSessionSummarizer
import org.ekstep.analytics.job.summarizer.GenieLaunchSummarizer
import org.ekstep.analytics.job.summarizer.RecommendationEngineJob
import org.ekstep.analytics.job.summarizer.ContentPopularitySummarizer
import org.ekstep.analytics.job.summarizer.DeviceContentUsageSummarizer
import org.ekstep.analytics.job.updater.LearnerContentActivityUpdater
import org.ekstep.analytics.job.updater.LearnerProfileUpdater
import org.ekstep.analytics.job.updater.ContentUsageUpdater
import org.ekstep.analytics.job.updater.ProficiencyUpdater
import org.ekstep.analytics.job.updater.LearnerSnapshotUpdater
import org.ekstep.analytics.job.updater.DeviceSpecificationUpdater
import org.ekstep.analytics.job.summarizer.ContentToVecJob
import org.ekstep.analytics.job.summarizer.CSVDumpJob
import org.ekstep.analytics.job.consolidated.RawTelemetryJobs
import org.ekstep.analytics.job.consolidated.SessionSummaryJobs
import org.ekstep.analytics.job.consolidated.RawTelemetryUpdaters
import org.ekstep.analytics.job.summarizer.DeviceRecommendationScoringJob
import org.ekstep.analytics.job.summarizer.StageSummarizer
import org.ekstep.analytics.job.summarizer.ItemUsageSummarizer
import org.ekstep.analytics.job.summarizer.GenieUsageSummarizer
import org.ekstep.analytics.job.updater.GenieUsageUpdater
import org.ekstep.analytics.job.summarizer.ItemSummarizer
import org.ekstep.analytics.job.updater.ItemSummaryUpdater
import org.ekstep.analytics.job.updater.ContentPopularityUpdater

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
            case "gusm" =>
                GenieUsageSummarizer
            case "gusmu" =>
                GenieUsageUpdater
            case "ism" =>
                ItemSummarizer
            case "ismu" =>
                ItemSummaryUpdater
            case _ =>
                throw new JobNotFoundException("Unknown job type found");
        }
    }
}