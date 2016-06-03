package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._

class TestDeviceSpecification extends SparkSpec(null) {

    "DeviceSpecification" should "generate devicespec and shouldn't throw any exception" in {

        val rdd = loadFile[Event]("src/test/resources/device-specification/raw.telemetry.test1.json");
        DeviceSpecification.execute(rdd, None);
    }
}