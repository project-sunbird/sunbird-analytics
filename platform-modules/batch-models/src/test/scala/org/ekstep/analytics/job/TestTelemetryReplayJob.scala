package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestTelemetryReplayJob extends SparkSpec(null) {

  val config = "{\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/telemetry-replay/data.json\"}]},\"model\":\"org.ekstep.analytics.job.UniqueBackupJob\",\"modelParams\":{\"contentId\":\"numeracy_382\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":true}}],\"parallelization\":10,\"appName\":\"UniqueBackupJob\",\"deviceMapping\":true}"

  "TelemetryReplayJob" should "Read and dispatch data properly" in {

    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val input = TelemetryReplayJob.getInputData(jobConfig)
    input.count() should be(37)

    val output = TelemetryReplayJob.dispatchData(jobConfig, input)
    output should be(37)

  }

}
