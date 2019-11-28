package org.ekstep.analytics.api.service

import akka.actor.{Actor, ActorRef}
import akka.pattern.pipe
import com.google.common.net.InetAddresses
import com.google.common.primitives.UnsignedInts
import com.typesafe.config.Config
import javax.inject.{Inject, Named}
import org.ekstep.analytics.api.util._
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.concurrent.{ExecutionContext, Future}

case class DeviceProfileRequest(did: String, headerIP: String)

class DeviceProfileService @Inject()(
                                      config: Config,
                                      redisUtil: RedisUtil,
                                      H2DB : H2DBUtil
                                    ) extends Actor {

  implicit val ec: ExecutionContext = context.system.dispatchers.lookup("device-register-actor")
  implicit val className: String ="DeviceProfileService"
  val deviceDatabaseIndex: Int = config.getInt("redis.deviceIndex")
  val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
  val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")
  implicit val jedisConnection: Jedis = redisUtil.getConnection(deviceDatabaseIndex)
  private val enableDebugLogging = config.getBoolean("device.api.enable.debug.log")

  override def preStart { println("starting DeviceProfileService") }

  override def postStop { println("Stopping DeviceProfileService") }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println(s"restarting DeviceProfileActor: $message")
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  def receive = {
    case deviceProfile: DeviceProfileRequest =>
      try {
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
      val did = deviceProfileRequest.did

      // logging resolved location details
      if (ipLocationFromH2.state.nonEmpty) {
        if (enableDebugLogging) {
          println(s"For IP: ${deviceProfileRequest.headerIP}, Location resolved for $did to state: ${ipLocationFromH2.state}, district: ${ipLocationFromH2.districtCustom}")
        }
        APILogger.log("", Option(Map("comments" -> s"Location resolved for $did to state: ${ipLocationFromH2.state}, district: ${ipLocationFromH2.districtCustom}")), "getDeviceProfile")
      } else {
        if (enableDebugLogging) {
          println(s"For IP: ${deviceProfileRequest.headerIP}, Location is not resolved for $did")
        }
        APILogger.log("", Option(Map("comments" -> s"Location is not resolved for $did")), "getDeviceProfile")
      }

      val deviceLocation = redisUtil.getAllByKey(did)
      val userDeclaredLoc = if (deviceLocation.nonEmpty && deviceLocation.get.getOrElse("user_declared_state", "").nonEmpty) {
        Option(Location(deviceLocation.get("user_declared_state"), deviceLocation.get("user_declared_district")))
      } else None

      if (enableDebugLogging) {
        userDeclaredLoc.foreach { declaredLocation => println(s"[did: $did, user_declared_state: ${declaredLocation.state}, user_declared_district: ${declaredLocation.district}") }
      }

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

    H2DB.readLocation(query)
  }

}
