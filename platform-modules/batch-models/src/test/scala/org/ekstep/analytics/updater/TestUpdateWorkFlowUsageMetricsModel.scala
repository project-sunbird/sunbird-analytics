package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestUpdateWorkFlowUsageMetricsModel extends SparkSpec(null) {

    "UpdateWorkFlowUsageMetricsModel" should "Should execute without exception " in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/device-profile/test-data1.log");
        UpdateWorkFlowUsageMetricsModel.execute(rdd, None)
    }
}
