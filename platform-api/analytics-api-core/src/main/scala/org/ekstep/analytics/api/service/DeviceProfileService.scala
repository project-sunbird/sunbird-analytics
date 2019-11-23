package org.ekstep.analytics.api.service

import akka.actor.{Actor, ActorRef}
import akka.pattern.pipe
import com.google.common.net.InetAddresses
import com.google.common.primitives.UnsignedInts
import com.typesafe.config.Config
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util.{APILogger, DeviceStateDistrict, H2DBUtil, RedisUtil}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.concurrent.{ExecutionContext, Future}

case class DeviceProfileRequest(did: String, headerIP: String)

class DeviceProfileService(saveMetricsActor: ActorRef, config: Config, redisUtil: RedisUtil) extends Actor {

  implicit val ec: ExecutionContext = context.system.dispatchers.lookup("device-register-actor")
  implicit val className: String ="DeviceProfileService"
  val metricsActor: ActorRef = saveMetricsActor
  val deviceDatabaseIndex: Int = config.getInt("redis.deviceIndex")
  val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
  val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")
  implicit val jedisConnection: Jedis = redisUtil.getConnection(deviceDatabaseIndex)
  private val logger = LogManager.getLogger("device-logger")

  def receive = {
    case deviceProfile: DeviceProfileRequest =>
      try {
        logger.info("DeviceProfile API Updater for device id " + deviceProfile.did)
        val senderActor = sender()
        val result = getDeviceProfile(deviceProfile)
        result.pipeTo(senderActor)
      } catch {
        case ex: JedisConnectionException =>
          ex.printStackTrace()
          val errorMessage = "Get DeviceProfileAPI failed due to " + ex.getMessage
          APILogger.log("", Option(Map("type" -> "api_access",
            "params" -> List(Map("status" -> 500, "method" -> "POST",
              "rid" -> "getDeviceProfile", "title" -> "getDeviceProfile")), "data" -> errorMessage)),
            "getDeviceProfile")
        case ex: Exception =>
          ex.printStackTrace()
          val errorMessage = "Get DeviceProfileAPI failed due to " + ex.getMessage
          APILogger.log("", Option(Map("type" -> "api_access",
            "params" -> List(Map("status" -> 500, "method" -> "POST",
              "rid" -> "getDeviceProfile", "title" -> "getDeviceProfile")), "data" -> errorMessage)),
            "getDeviceProfile")
      }
  }

  def getDeviceProfile(deviceProfileRequest: DeviceProfileRequest): Future[Option[DeviceProfile]] = {

    if (deviceProfileRequest.headerIP.nonEmpty) {
      val ipLocationFromH2 = resolveLocationFromH2(deviceProfileRequest.headerIP)

      // logging resolved location details
      if (ipLocationFromH2.state.nonEmpty && ipLocationFromH2.districtCustom.nonEmpty) {
        println(s"For IP: ${deviceProfileRequest.headerIP}, Location resolved for ${deviceProfileRequest.did} to state: ${ipLocationFromH2.state}, district: ${ipLocationFromH2.districtCustom}")
        APILogger.log("", Option(Map("comments" -> s"Location resolved for ${deviceProfileRequest.did} to state: ${ipLocationFromH2.state}, district: ${ipLocationFromH2.districtCustom}")), "getDeviceProfile")
      } else {
        println(s"For IP: ${deviceProfileRequest.headerIP}, Location is not resolved for ${deviceProfileRequest.did}")
        APILogger.log("", Option(Map("comments" -> s"Location is not resolved for ${deviceProfileRequest.did}")), "getDeviceProfile")
      }

      val deviceLocation = redisUtil.getAllByKey(deviceProfileRequest.did)
      val userDeclaredLoc = if (deviceLocation.nonEmpty && deviceLocation.get.getOrElse("user_declared_state", "").nonEmpty) {
        Option(Location(deviceLocation.get("user_declared_state"), deviceLocation.get("user_declared_district")))
      } else None

      Future(Some(DeviceProfile(userDeclaredLoc, Option(Location(ipLocationFromH2.state, ipLocationFromH2.districtCustom)))))
    } else {
      Future(None)
    }
  }

  def resolveLocationFromH2(ipAddress: String): DeviceStateDistrict = {
    val ipAddressInt: Long = UnsignedInts.toLong(InetAddresses.coerceToInteger(InetAddresses.forString(ipAddress)))

    val query =
      s"""
         |SELECT
         |  glc.subdivision_1_name state,
         |  glc.subdivision_2_custom_name district_custom
         |FROM $geoLocationCityIpv4TableName gip,
         |  $geoLocationCityTableName glc
         |WHERE gip.geoname_id = glc.geoname_id
         |  AND gip.network_start_integer <= $ipAddressInt
         |  AND gip.network_last_integer >= $ipAddressInt
               """.stripMargin

    H2DBUtil.readLocation(query)
  }

}