package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import akka.actor.{Actor, ActorRef}
import com.google.common.net.InetAddresses
import com.typesafe.config.Config
import com.google.common.primitives.UnsignedInts
import is.tagomor.woothee.Classifier
import org.apache.logging.log4j.LogManager
import org.postgresql.util.PSQLException

import scala.concurrent.ExecutionContext

case class RegisterDevice(did: String, headerIP: String, ip_addr: Option[String] = None, fcmToken: Option[String] = None, producer: Option[String] = None, dspec: Option[String] = None, uaspec: Option[String] = None, firstAccess: Option[Long]= None)
case class DeviceProfileLog(device_id: String, location: DeviceLocation, device_spec: Option[Map[String, AnyRef]] = None, uaspec: Option[String] = None, fcm_token: Option[String] = None, producer_id: Option[String] = None, firstAccess: Option[Long] = None)

class DeviceRegisterService(saveMetricsActor: ActorRef, config: Config) extends Actor {

    implicit val ec: ExecutionContext = context.system.dispatchers.lookup("device-register-actor")
    implicit val className: String ="DeviceRegisterService"
    val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
    val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")
    val metricsActor: ActorRef = saveMetricsActor
    private val logger = LogManager.getLogger("device-logger")


    def receive = {
        case deviceRegDetails: RegisterDevice =>
            try {
                metricsActor.tell(IncrementApiCalls, ActorRef.noSender)
                // registerDevice(registrationDetails.did, registrationDetails.headerIP, registrationDetails.ip_addr, registrationDetails.fcmToken, registrationDetails.producer, registrationDetails.dspec, registrationDetails.uaspec)
                registerDevice(deviceRegDetails)
            } catch {
                case ex: PSQLException =>
                    ex.printStackTrace()
                    val errorMessage = "DeviceRegisterAPI failed due to " + ex.getMessage
                    metricsActor.tell(IncrementLocationDbErrorCount, ActorRef.noSender)
                    APILogger.log("", Option(Map("type" -> "api_access",
                        "params" -> List(Map("status" -> 500, "method" -> "POST",
                            "rid" -> "registerDevice", "title" -> "registerDevice")), "data" -> errorMessage)),
                        "registerDevice")
            }
    }


    def registerDevice(registrationDetails: RegisterDevice) {
        val validIp = if (registrationDetails.headerIP.startsWith("192")) registrationDetails.ip_addr.getOrElse("") else registrationDetails.headerIP
        if (validIp.nonEmpty) {
            val location = resolveLocation(validIp)

            // logging metrics
            if(isLocationResolved(location)) {
                APILogger.log("", Option(Map("comments" -> s"Location resolved for ${registrationDetails.did} to state: ${location.state}, city: ${location.city}, district: ${location.districtCustom}")), "registerDevice")
                metricsActor.tell(IncrementLocationDbSuccessCount, ActorRef.noSender)
            } else {
                APILogger.log("", Option(Map("comments" -> s"Location is not resolved for ${registrationDetails.did}")), "registerDevice")
                metricsActor.tell(IncrementLocationDbMissCount, ActorRef.noSender)
            }

            val deviceSpec: Map[String, AnyRef] = registrationDetails.dspec match {
                case Some(value) => JSONUtils.deserialize[Map[String, AnyRef]](value)
                case None => Map()
            }

            val deviceProfileLog = DeviceProfileLog(registrationDetails.did, location, Option(deviceSpec),
              registrationDetails.uaspec, registrationDetails.fcmToken, registrationDetails.producer, registrationDetails.firstAccess)

            val deviceRegisterLogEvent = generateDeviceRegistrationLogEvent(deviceProfileLog)
            logger.info(deviceRegisterLogEvent)
            metricsActor.tell(IncrementLogDeviceRegisterSuccessCount, ActorRef.noSender)
        }

    }

    def resolveLocation(ipAddress: String): DeviceLocation = {
        val ipAddressInt: Long = UnsignedInts.toLong(InetAddresses.coerceToInteger(InetAddresses.forString(ipAddress)))

        val query =
            s"""
               |SELECT
               |  glc.continent_name,
               |  glc.country_iso_code country_code,
               |  glc.country_name,
               |  glc.subdivision_1_iso_code state_code,
               |  glc.subdivision_1_name state,
               |  glc.subdivision_2_name sub_div_2,
               |  glc.city_name city,
               |  glc.subdivision_1_custom_name state_custom,
               |  glc.subdivision_1_custom_code state_code_custom,
               |  glc.subdivision_2_custom_name district_custom
               |FROM $geoLocationCityIpv4TableName gip,
               |  $geoLocationCityTableName glc
               |WHERE glc.country_iso_code = 'IN'
               |  AND gip.geoname_id = glc.geoname_id
               |  AND gip.network_start_integer <= $ipAddressInt
               |  AND gip.network_last_integer >= $ipAddressInt
               """.stripMargin

        metricsActor.tell(IncrementLocationDbHitCount, ActorRef.noSender)
        PostgresDBUtil.readLocation(query).headOption.getOrElse(new DeviceLocation())
    }

    def isLocationResolved(loc: DeviceLocation): Boolean = {
        Option(loc.state).nonEmpty
    }

    def parseUserAgent(uaspec: Option[String]): Option[String] = {
        uaspec.map {
            userAgent =>
                val uaspecMap = Classifier.parse(userAgent)
                val parsedUserAgentMap = Map("agent" -> uaspecMap.get("name"), "ver" -> uaspecMap.get("version"),
                    "system" -> uaspecMap.get("os"), "raw" -> userAgent)
                val uaspecStr = JSONUtils.serialize(parsedUserAgentMap).replaceAll("\"", "'")
                uaspecStr
        }
    }

    def generateDeviceRegistrationLogEvent(result: DeviceProfileLog): String = {

        val uaspecStr = parseUserAgent(result.uaspec)
        val deviceProfile: Map[String, Any] =
          Map("device_id" -> s"${result.device_id}",
            "country_code" -> s"${result.location.countryCode}",
            "country" -> s"${result.location.countryName}",
            "state_code" -> s"${result.location.stateCode}",
            "state" -> s"${result.location.state}",
            "city" -> s"${result.location.city}",
            "state_custom" -> s"${result.location.stateCustom}",
            "state_code_custom" -> s"${result.location.stateCodeCustom}",
            "district_custom" -> s"${result.location.districtCustom}",
            "device_spec" -> result.device_spec.map(x => JSONUtils.serialize(x.mapValues(_.toString)).replaceAll("\"", "'")).orNull,
            "uaspec" -> uaspecStr.orNull,
            "fcm_token" -> result.fcm_token.orNull,
            "producer_id" -> result.producer_id.orNull,
            "api_last_updated_on" -> DateTime.now(DateTimeZone.UTC).getMillis,
            "first_access" -> result.firstAccess.orNull
          )
        JSONUtils.serialize(deviceProfile)
    }

}
