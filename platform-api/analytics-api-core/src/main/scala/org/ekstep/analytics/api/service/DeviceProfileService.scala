package org.ekstep.analytics.api.service

import akka.actor.{Actor, ActorRef}
import com.google.common.net.InetAddresses
import com.google.common.primitives.UnsignedInts
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.{APILogger, DeviceStateDistrict, H2DBUtil, RedisUtil}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.JavaConverters._

case class DeviceProfileRequest(did: String, headerIP: String)

class DeviceProfileService(saveMetricsActor: ActorRef, config: Config, redisUtil: RedisUtil) extends Actor {

  implicit val className: String ="DeviceProfileService"
  val metricsActor: ActorRef = saveMetricsActor
  val deviceDatabaseIndex: Int = config.getInt("redis.deviceIndex")
  val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
  val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")
  private val enableDebugLogging = config.getBoolean("device.api.enable.debug.log")

  def receive = {
    case deviceProfile: DeviceProfileRequest =>
      try {
        sender() ! getDeviceProfile(deviceProfile)
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

  def getDeviceProfile(deviceProfileRequest: DeviceProfileRequest): Option[DeviceProfile] = {

    if (deviceProfileRequest.headerIP.nonEmpty) {
      val ipLocationFromH2 = resolveLocationFromH2(deviceProfileRequest.headerIP)
      val did = deviceProfileRequest.did

      // logging resolved location details
      if (ipLocationFromH2.state.nonEmpty) {
        if (enableDebugLogging) {
          println(s"For IP: ${deviceProfileRequest.headerIP}, IP Location resolved for $did to state: ${ipLocationFromH2.state}, district: ${ipLocationFromH2.districtCustom}")
        }
        APILogger.log("", Option(Map("comments" -> s"IP Location resolved for $did to state: ${ipLocationFromH2.state}, district: ${ipLocationFromH2.districtCustom}")), "getDeviceProfile")
      } else {
        if (enableDebugLogging) {
          println(s"For IP: ${deviceProfileRequest.headerIP}, IP Location is not resolved for $did")
        }
        APILogger.log("", Option(Map("comments" -> s"IP Location is not resolved for $did")), "getDeviceProfile")
      }

      val jedisConnection: Jedis = redisUtil.getConnection(deviceDatabaseIndex)
      val deviceLocation = try {
        Option(jedisConnection.hgetAll(did).asScala.toMap)
      } catch {
        case ex: Exception => {
          APILogger.log("", Option(Map("comments" -> s"Redis exception during did lookup: ${ex.getMessage}")), "DeviceProfileService")
          None
        }
      } finally {
        jedisConnection.close()
      }

      val userDeclaredLoc = if (deviceLocation.nonEmpty && deviceLocation.get.getOrElse("user_declared_state", "").nonEmpty) {
        Option(Location(deviceLocation.get("user_declared_state"), deviceLocation.get("user_declared_district")))
      } else None

      if (enableDebugLogging) {
        userDeclaredLoc.foreach {
          declaredLocation => {
            APILogger.log("", Option(Map("comments" -> s"[did: $did, user_declared_state: ${declaredLocation.state}, user_declared_district: ${declaredLocation.district}")), "getDeviceProfile")
            println(s"[did: $did, user_declared_state: ${declaredLocation.state}, user_declared_district: ${declaredLocation.district}")
          }
        }
      }

      Some(DeviceProfile(userDeclaredLoc, Option(Location(ipLocationFromH2.state, ipLocationFromH2.districtCustom))))
    } else {
      None
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
