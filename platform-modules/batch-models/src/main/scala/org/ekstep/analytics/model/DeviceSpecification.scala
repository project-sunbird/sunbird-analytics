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
        val filteredEvents = DataFilter.filter(events, Filter("edata.eks.dspec", "ISNOTNULL", None));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val deviceSummary = filteredEvents.map { event =>
            val deviceSpec = event.edata.eks.dspec.asInstanceOf[Map[String, AnyRef]]
            val deviceId = event.did
            val deviceName = deviceSpec.get("dname").get.asInstanceOf[String]
            val deviceLocalName = deviceSpec.get("dlocname").get.asInstanceOf[String]
            val os = deviceSpec.get("os").get.asInstanceOf[String]
            val make = deviceSpec.get("make").get.asInstanceOf[String]
            val memory = deviceSpec.get("mem").get.asInstanceOf[Double]
            val internalDisk = deviceSpec.get("idisk").get.asInstanceOf[Double]
            val externalDisk = deviceSpec.get("edisk").get.asInstanceOf[Double]
            val screenSize = deviceSpec.get("scrn").get.asInstanceOf[Double]
            val primarySecondaryCamera = deviceSpec.get("camera").get.asInstanceOf[String]
            val cpu = deviceSpec.get("cpu").get.asInstanceOf[String]
            val numSims = deviceSpec.get("sims").get.asInstanceOf[Double]
            val capabilities = deviceSpec.get("cap").get.asInstanceOf[List[String]]

            DeviceSpec(deviceId, deviceName, deviceLocalName, os, make, memory, internalDisk, externalDisk, screenSize, primarySecondaryCamera, cpu, numSims, capabilities)

        }

        deviceSummary.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE);
        deviceSummary.map { x => JSONUtils.serialize(x) };
    }

}