package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.ekstep.analytics.updater.UpdateDeviceSpecificationDB

class TestDeviceSpecification extends SparkSpec(null) {

    "UpdateDeviceSpecification" should "generate devicespec and shouldn't throw any exception" in {

        val rdd = loadFile[ProfileEvent]("src/test/resources/device-specification/raw.telemetry.test1.json");
        UpdateDeviceSpecificationDB.execute(rdd, None);
    }
}