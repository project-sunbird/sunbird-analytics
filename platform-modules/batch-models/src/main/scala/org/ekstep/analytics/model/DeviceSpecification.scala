package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils

case class DeviceSpec(device_id: String, device_name: String, device_local_name: String, os: String, make: String,
                      memory: Double, internal_disk: Double, external_disk: Double, screen_size: Double,
                      primary_secondary_camera: String, cpu: String, num_sims: Double, capabilities: List[String])

object DeviceSpecification extends IBatchModel[Event] with Serializable {

    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val events = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_GENIE_START")));
        val filteredEvents = DataFilter.filter(DataFilter.filter(events, Filter("edata", "ISNOTNULL", None)), Filter("edata.eks.dspec", "ISNOTNULL", None));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val deviceSummary = filteredEvents.map { event =>
            val deviceSpec = event.edata.eks.dspec.asInstanceOf[Map[String, AnyRef]]
            val deviceId = event.did
            val deviceName = deviceSpec.getOrElse("dname", "").asInstanceOf[String]
            val deviceLocalName = deviceSpec.getOrElse("dlocname", "").asInstanceOf[String]
            val os = deviceSpec.getOrElse("os", "").asInstanceOf[String]
            val make = deviceSpec.getOrElse("make", "").asInstanceOf[String]
            val memory = deviceSpec.getOrElse("mem", 0d).asInstanceOf[Double]
            val internalDisk = deviceSpec.getOrElse("idisk", 0d).asInstanceOf[Double]
            val externalDisk = deviceSpec.getOrElse("edisk", 0d).asInstanceOf[Double]
            val screenSize = deviceSpec.getOrElse("scrn", 0d).asInstanceOf[Double]
            val primarySecondaryCamera = deviceSpec.getOrElse("camera", "").asInstanceOf[String]
            val cpu = deviceSpec.getOrElse("cpu", "").asInstanceOf[String]
            val numSims = deviceSpec.getOrElse("sims", "").asInstanceOf[Double]
            val capabilities = deviceSpec.getOrElse("cap", "").asInstanceOf[List[String]]

            DeviceSpec(deviceId, deviceName, deviceLocalName, os, make, memory, internalDisk, externalDisk, screenSize, primarySecondaryCamera, cpu, numSims, capabilities)
        }.cache();

        deviceSummary.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE);
        deviceSummary.map { x => JSONUtils.serialize(x) };
    }

}