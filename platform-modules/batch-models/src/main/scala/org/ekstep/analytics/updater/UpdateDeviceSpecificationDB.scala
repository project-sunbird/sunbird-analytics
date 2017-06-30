package org.ekstep.analytics.updater

import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import scala.reflect.runtime.universe
import org.joda.time.DateTime
import org.ekstep.analytics.framework.conf.AppConf

case class DeviceSpec(device_id: String, app_id: String, channel_id: String, device_name: String, device_local_name: String, os: String, make: String,
                      memory: Double, internal_disk: Double, external_disk: Double, screen_size: Double,
                      primary_secondary_camera: String, cpu: String, num_sims: Double, capabilities: List[String], updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with Output

object UpdateDeviceSpecificationDB extends IBatchModelTemplate[ProfileEvent, ProfileEvent, DeviceSpec, UpdaterOutput] with Serializable {

    val className = "org.ekstep.analytics.model.UpdateDeviceSpecificationDB"
    override def name: String = "UpdateDeviceSpecificationDB"

    override def preProcess(data: RDD[ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ProfileEvent] = {
        val events = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_GENIE_START")));
        DataFilter.filter(DataFilter.filter(events, Filter("edata", "ISNOTNULL", None)), Filter("edata.eks.dspec", "ISNOTNULL", None));
    }

    override def algorithm(data: RDD[ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceSpec] = {

        data.map { event =>
            val deviceSpec = event.edata.eks.dspec.asInstanceOf[Map[String, AnyRef]]
            val deviceId = event.did
            val appId = event.appid.getOrElse(AppConf.getConfig("default.app.id"));
            val channelId = event.channelid.getOrElse(AppConf.getConfig("default.channel.id"));
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

            DeviceSpec(deviceId, appId, channelId, deviceName, deviceLocalName, os, make, memory, internalDisk, externalDisk, screenSize, primarySecondaryCamera, cpu, numSims, capabilities)
        }.cache();
    }

    override def postProcess(data: RDD[DeviceSpec], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[UpdaterOutput] = {
        data.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE);
        sc.parallelize(Seq(UpdaterOutput("Device specification database updated - " + data.count())));
    }
}