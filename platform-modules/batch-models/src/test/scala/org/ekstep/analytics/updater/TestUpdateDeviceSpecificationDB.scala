package org.ekstep.analytics.updater

import org.ekstep.analytics.framework._
import org.ekstep.analytics.updater.UpdateDeviceSpecificationDB
import org.ekstep.analytics.model.SparkSpec

class TestUpdateDeviceSpecificationDB extends SparkSpec(null) {

    "UpdateDeviceSpecificationDB" should "generate devicespec and shouldn't throw any exception" in {

        val rdd = loadFile[ProfileEvent]("src/test/resources/device-specification/raw.telemetry.test1.json");
        UpdateDeviceSpecificationDB.execute(rdd, None);
    }
}