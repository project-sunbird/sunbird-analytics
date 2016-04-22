package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.DeviceSpecification
import org.ekstep.analytics.framework.TelemetryEventV2
import org.apache.spark.SparkContext

object DeviceSpecificationUpdater {
  def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[TelemetryEventV2]("batch", config, DeviceSpecification);
    }
}