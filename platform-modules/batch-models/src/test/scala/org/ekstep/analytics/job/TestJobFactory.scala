package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.job.consolidated.RawTelemetryJobs
import org.ekstep.analytics.job.consolidated.SessionSummaryJobs
import org.ekstep.analytics.job.consolidated.RawTelemetryUpdaters

class TestJobFactory extends BaseSpec {

    it should "return a Model class for a model code" in {

        val jobCUS = JobFactory.getJob("cus")
        jobCUS should be(ContentUsageSummarizer)
        jobCUS.isInstanceOf[IJob] should be(true)

        val jobCPS = JobFactory.getJob("cps")
        jobCPS should be(ContentPopularitySummarizer)
        jobCPS.isInstanceOf[IJob] should be(true)

        val jobCPU = JobFactory.getJob("cpu")
        jobCPU should be(ContentPopularityUpdater)
        jobCPU.isInstanceOf[IJob] should be(true)

        val jobCUU = JobFactory.getJob("cuu")
        jobCUU should be(ContentUsageUpdater)
        jobCUU.isInstanceOf[IJob] should be(true)

        val jobGSS = JobFactory.getJob("gss")
        jobGSS should be(GenieUsageSessionSummarizer)
        jobGSS.isInstanceOf[IJob] should be(true)

        val jobGLS = JobFactory.getJob("gls")
        jobGLS should be(GenieLaunchSummarizer)
        jobGLS.isInstanceOf[IJob] should be(true)

        val jobDUS = JobFactory.getJob("dus")
        jobDUS should be(DeviceUsageSummarizer)
        jobDUS.isInstanceOf[IJob] should be(true)

        val jobCSS = JobFactory.getJob("css")
        jobCSS should be(ContentSideloadingSummarizer)
        jobCSS.isInstanceOf[IJob] should be(true)

        val jobLPU = JobFactory.getJob("lpu")
        jobLPU should be(LearnerProfileUpdater)
        jobLPU.isInstanceOf[IJob] should be(true)

        val jobCMU = JobFactory.getJob("cmu")
        jobCMU should be(ContentModelUpdater)
        jobCMU.isInstanceOf[IJob] should be(true)

        val jobDSU = JobFactory.getJob("dsu")
        jobDSU should be(DeviceSpecificationUpdater)
        jobDSU.isInstanceOf[IJob] should be(true)

        val jobIS = JobFactory.getJob("is")
        jobIS should be(ItemUsageSummarizer)
        jobIS.isInstanceOf[IJob] should be(true)

        val jobDCUS = JobFactory.getJob("dcus")
        jobDCUS should be(DeviceContentUsageSummarizer)
        jobDCUS.isInstanceOf[IJob] should be(true)

        val jobCSV = JobFactory.getJob("csv")
        jobCSV should be(CSVDumpJob)
        jobCSV.isInstanceOf[IJob] should be(true)

        val jobCTV = JobFactory.getJob("ctv")
        jobCTV should be(ContentToVecJob)
        jobCTV.isInstanceOf[IJob] should be(true)

        val jobDRT = JobFactory.getJob("device-recos-training")
        jobDRT should be(DeviceRecommendationTrainingJob)
        jobDRT.isInstanceOf[IJob] should be(true)

        val jobDRS = JobFactory.getJob("device-recos-scoring")
        jobDRS should be(DeviceRecommendationScoringJob)
        jobDRS.isInstanceOf[IJob] should be(true)

        val jobRTJ = JobFactory.getJob("raw-telemetry-jobs")
        jobRTJ should be(RawTelemetryJobs)
        jobRTJ.isInstanceOf[IJob] should be(true)

        val jobSSJ = JobFactory.getJob("ss-jobs")
        jobSSJ should be(SessionSummaryJobs)
        jobSSJ.isInstanceOf[IJob] should be(true)

        val jobRTU = JobFactory.getJob("raw-telemetry-updaters")
        jobRTU should be(RawTelemetryUpdaters)
        jobRTU.isInstanceOf[IJob] should be(true)

        val sts = JobFactory.getJob("sts")
        sts should be(StageSummarizer)
        sts.isInstanceOf[IJob] should be(true)
        
        val jobGLsumm = JobFactory.getJob("genie-launch-summ")
        jobGLsumm should be(GenieUsageSummarizer)
        jobGLsumm.isInstanceOf[IJob] should be(true)

        val jobGLM = JobFactory.getJob("genie-launch-metrics")
        jobGLM should be(GenieUsageUpdater)
        jobGLM.isInstanceOf[IJob] should be(true)

        val jobISS = JobFactory.getJob("item-usage-summ")
        jobISS should be(ItemSummarizer)
        jobISS.isInstanceOf[IJob] should be(true)

    }
}