package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.DeviceSpecification
import org.ekstep.analytics.framework.Event
import org.apache.spark.SparkContext
import optional.Application

object DeviceSpecificationUpdater extends Application {
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[Event]("batch", config, DeviceSpecification);
    }
}