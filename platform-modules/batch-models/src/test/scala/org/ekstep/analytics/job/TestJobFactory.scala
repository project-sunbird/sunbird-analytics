package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.job.consolidated._
import org.ekstep.analytics.views.PrecomputedViewsJob
import org.ekstep.analytics.vidyavaani.job._

class TestJobFactory extends BaseSpec {

    it should "return a Model class for a model code" in {

        val jobIds = List("as", "ss", "las", "lp", "ls", "lcas", "lcr", "cus", "cps", "cpu", "cuu", "gss", "gls",
            "dus", "css", "lpu", "cmu", "dsu", "is", "dcus", "csv", "ctv", "device-recos-training",
            "device-recos-scoring", "raw-telemetry-jobs", "ss-jobs", "raw-telemetry-updaters", "sts",
            "genie-launch-summ", "genie-launch-metrics", "item-usage-summ", "item-usage-metrics", "gsts", "gfs",
            "gfa", "data-exhaust", "precomp-views", "content-recos", "content-own-rel", "content-lang-rel",
            "content-asset-rel", "concept-lan-rel", "vidyavaani-jobs", "consumption-metrics", "creation-metrics",
            "eocfs", "re-enrichment-model", "creation-reco", "content-snapshot", "concept-snapshot", "asset-snapshot",
            "asset-snapshot-updater", "content-snapshot-updater", "concept-snapshot-updater", "ce-ss", "ce-usage",
            "ce-usage-updater", "content-creation-metrics", "textbook-snapshot-updater", "app-ss", "obj-cache-updater",
            "textbook-ss", "textbook-usage", "textbook-usage-updater", "author-usage-summary", "author-usage-updater",
            "app-usage", "app-usage-updater", "publish-pipeline-summ", "app-raw-telemetry-jobs", "app-raw-telemetry-updaters",
            "publish-pipeline-updater")

        val jobs = jobIds.map { f => JobFactory.getJob(f) }

        jobs(0) should be(AserScreenSummarizer)
        jobs(0).isInstanceOf[IJob] should be(true)

        jobs(22) should be(DeviceRecommendationTrainingJob)
        jobs(22).isInstanceOf[IJob] should be(true)

        jobs(71) should be(PublishPipelineUpdater)
        jobs(71).isInstanceOf[IJob] should be(true)
    }
}