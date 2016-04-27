package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._

class TestDeviceSpecification extends SparkSpec(null) {

    "DeviceSpecification" should "generate devicespec and pass all positive test cases" in {

        val rdd = loadFile[TelemetryEventV2]("src/test/resources/device-specification/raw.telemetry.test1.json");
        DeviceSpecification.execute(rdd, None);
    }
}