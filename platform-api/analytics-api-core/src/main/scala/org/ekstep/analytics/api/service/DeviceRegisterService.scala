package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util._
import org.ekstep.analytics.api._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import akka.actor.{Actor, ActorRef}
import com.google.common.net.InetAddresses
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.exceptions.DriverException
import com.google.common.primitives.UnsignedInts
import is.tagomor.woothee.Classifier
import org.apache.logging.log4j.LogManager
import org.postgresql.util.PSQLException

import scala.concurrent.ExecutionContext

case class RegisterDevice(did: String, headerIP: String, ip_addr: Option[String], fcmToken: Option[String], producer: Option[String], dspec: Option[String], uaspec: Option[String])

class DeviceRegisterService(saveMetricsActor: ActorRef) extends Actor {

    implicit val ec: ExecutionContext = context.system.dispatchers.lookup("device-register-actor")
    implicit val className: String ="DeviceRegisterService"
    val config: Config = ConfigFactory.load()
    val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
    val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")
    val metricsActor: ActorRef = saveMetricsActor //context.system.actorOf(Props[SaveMetricsActor])
    private val logger = LogManager.getLogger("device-logger")


    def receive = {
        case RegisterDevice(did: String, headerIP: String, ip_addr: Option[String], fcmToken: Option[String], producer: Option[String], dspec: Option[String], uaspec: Option[String]) =>
            try {
                metricsActor.tell(IncrementApiCalls, ActorRef.noSender)
                registerDevice(did, headerIP, ip_addr, fcmToken, producer, dspec, uaspec)
            } catch {
                case ex: PSQLException =>
                    ex.printStackTrace()
                    val errorMessage = "DeviceRegisterAPI failed due to " + ex.getMessage
                    metricsActor.tell(IncrementLocationDbErrorCount, ActorRef.noSender)
                    APILogger.log("", Option(Map("type" -> "api_access",
                        "params" -> List(Map("status" -> 500, "method" -> "POST",
                            "rid" -> "registerDevice", "title" -> "registerDevice")), "data" -> errorMessage)),
                        "registerDevice")
                case ex: DriverException =>
                    ex.printStackTrace()
                    val errorMessage = "DeviceRegisterAPI failed due to " + ex.getMessage
                    metricsActor.tell(IncrementDeviceDbSaveErrorCount, ActorRef.noSender)
                    APILogger.log("", Option(Map("type" -> "api_access",
                        "params" -> List(Map("status" -> 500, "method" -> "POST",
                            "rid" -> "registerDevice", "title" -> "registerDevice")), "data" -> errorMessage)),
                        "registerDevice")
            }
    }

    def registerDevice(did: String, headerIP: String, ip_addr: Option[String], fcmToken: Option[String], producer: Option[String], dspec: Option[String], uaspec: Option[String]) = {
        val validIp = if (headerIP.startsWith("192")) ip_addr.getOrElse("") else headerIP
        if (validIp.nonEmpty) {
            val location = resolveLocation(validIp)

            // logging metrics
            if(isLocationResolved(location)) {
                APILogger.log("", Option(Map("comments" -> s"Location resolved for $did to state: ${location.state} and city: ${location.city}")), "registerDevice")
                metricsActor.tell(IncrementLocationDbSuccessCount, ActorRef.noSender)
            }
            else {
                APILogger.log("", Option(Map("comments" -> s"Location is not resolved for $did")), "registerDevice")
                metricsActor.tell(IncrementLocationDbMissCount, ActorRef.noSender)
            }

            val deviceSpec: Map[String, AnyRef] = dspec match {
                case Some(value) => JSONUtils.deserialize[Map[String, AnyRef]](value)
                case None => Map()
            }

            updateDeviceProfileLog(
                did,
                Option(location.countryCode).map(_.trim).filterNot(_.isEmpty),
                Option(location.countryName).map(_.trim).filterNot(_.isEmpty),
                Option(location.stateCode).map(_.trim).filterNot(_.isEmpty),
                Option(location.state).map(_.trim).filterNot(_.isEmpty),
                Option(location.city).map(_.trim).filterNot(_.isEmpty),
                Option(location.stateCustom).map(_.trim).filterNot(_.isEmpty),
                Option(location.stateCodeCustom).map(_.trim).filterNot(_.isEmpty),
                Option(location.districtCustom).map(_.trim).filterNot(_.isEmpty),
                Option(deviceSpec),
                uaspec.map(_.trim).filterNot(_.isEmpty),
                fcmToken,
                producer
            )

            // updateDeviceFirstAccess(did)
        }

        metricsActor.tell(IncrementDeviceDbSaveSuccessCount, ActorRef.noSender)
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

    def updateDeviceProfileLog(did: String, countryCode: Option[String], country: Option[String],
                            stateCode: Option[String], state: Option[String], city: Option[String],
                            stateCustom: Option[String], stateCodeCustom: Option[String], districtCustom: Option[String],
                            deviceSpec: Option[Map[String, AnyRef]], uaspec: Option[String], fcmToken: Option[String], producer: Option[String]) {

        val uaspecStr = parseUserAgent(uaspec)
        val device_profile: Map[String, Any] = Map("device_id" -> s"'$did'",
            "country_code" -> s"'${countryCode.getOrElse("")}'", "country" -> s"'${country.getOrElse("")}'",
            "state_code" -> s"'${stateCode.getOrElse("")}'", "state" -> s"'${state.getOrElse("")}'", "city" -> s"'${city.getOrElse("")}'",
            "state_custom" -> s"'${stateCustom.getOrElse("")}'","state_code_custom" -> s"'${stateCodeCustom.getOrElse("")}'",
            "district_custom" -> s"'${districtCustom.getOrElse("")}'",
            "device_spec" -> deviceSpec.map(x => JSONUtils.serialize(x.mapValues(_.toString))
              .replaceAll("\"", "'")).getOrElse(Map()),
            "uaspec" -> uaspecStr.getOrElse(""), "fcm_token" -> s"'${fcmToken.getOrElse("")}'", "producer_id" -> s"'${producer.getOrElse("")}'", "updated_date" -> DateTime.now(DateTimeZone.UTC).getMillis)

        logger.info(JSONUtils.serialize(device_profile))
    }

    def updateDeviceFirstAccess(did: String): Unit = {
        val query =
            s"""
               |UPDATE ${Constants.DEVICE_DB}.${Constants.DEVICE_PROFILE_TABLE}
               | SET first_access = ${new DateTime(DateTimeZone.UTC).getMillis}
               | WHERE device_id = '$did' IF first_access = null
           """.stripMargin
        DBUtil.session.execute(query)
    }
}
