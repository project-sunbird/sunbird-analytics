package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.summarizer.ContentUsageSummarizer
import org.ekstep.analytics.job.summarizer.DeviceUsageSummarizer
import org.ekstep.analytics.job.summarizer.LearnerActivitySummarizer
import org.ekstep.analytics.job.summarizer.AserScreenSummarizer
import org.ekstep.analytics.job.summarizer.ItemSummarizer
import org.ekstep.analytics.job.summarizer.ContentSideloadingSummarizer
import org.ekstep.analytics.job.summarizer.LearnerSessionSummarizer
import org.ekstep.analytics.job.summarizer.DeviceRecommendationJob
import org.ekstep.analytics.job.summarizer.GenieUsageSessionSummarizer
import org.ekstep.analytics.job.summarizer.GenieLaunchSummarizer
import org.ekstep.analytics.job.summarizer.RecommendationEngineJob
import org.ekstep.analytics.job.summarizer.ContentPopularitySummarizer
import org.ekstep.analytics.job.summarizer.DeviceContentUsageSummarizer
import org.ekstep.analytics.job.updater.LearnerContentActivityUpdater
import org.ekstep.analytics.job.updater.LearnerProfileUpdater
import org.ekstep.analytics.job.updater.ContentUsageUpdaterJob
import org.ekstep.analytics.job.updater.ProficiencyUpdater
import org.ekstep.analytics.job.updater.LearnerSnapshotUpdater
import org.ekstep.analytics.job.updater.DeviceSpecificationUpdater
import org.ekstep.analytics.job.summarizer.ContentToVecJob
import org.ekstep.analytics.job.summarizer.CSVDumpJob

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
            case "cuu" =>
                ContentUsageUpdaterJob
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
                ItemSummarizer
            case "dcus" =>
                DeviceContentUsageSummarizer
            case "csv" => 
                CSVDumpJob
            case "ctv" =>
                ContentToVecJob
            case "device-recos" =>
                DeviceRecommendationJob  
            case "re-scoring" =>
                REScoringJob
            case _ =>
                throw new JobNotFoundException("Unknown job type found");
        }
    }
}