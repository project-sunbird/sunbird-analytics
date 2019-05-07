package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util._
import org.ekstep.analytics.api._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import akka.actor.Actor
import com.google.common.net.InetAddresses
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.datastax.driver.core.ResultSet
import com.google.common.primitives.UnsignedInts
import is.tagomor.woothee.Classifier
import org.ekstep.analytics.framework.util.JobLogger

case class RegisterDevice(did: String, ip: String, request: String, uaspec: Option[String])

class DeviceRegisterService extends Actor {
    implicit val className: String ="DeviceRegisterService"
    val config: Config = ConfigFactory.load()
    val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
    val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")
    val defaultChannel: String = config.getString("default.channel")

    def receive = {
      case RegisterDevice(did: String, ip: String, request: String, uaspec: Option[String]) => {
        try {
          registerDevice(did, ip, request, uaspec)
        } catch {
          case e: Exception => {
            val errorMessage = "DeviceRegisterAPI failed due to " + e.getMessage
            APILogger.log("", Option(Map("type" -> "api_access", "params" -> List(Map("status" -> 500, "method" -> "POST", "rid" -> "registerDevice", "title" -> "registerDevice")), "data" -> errorMessage)), "registerDevice")
          }
        }
      }
    }

    def registerDevice(did: String, ipAddress: String, request: String, uaspec: Option[String]): String = {
        val body = JSONUtils.deserialize[RequestBody](request)
        val validIp = if (ipAddress.startsWith("192")) body.request.ip_addr.getOrElse("") else ipAddress
        println(s"did: $did | device_spec: ${body.request.dspec} | uaspec: ${uaspec.getOrElse("")}")
        if (validIp.nonEmpty) {
            val location = resolveLocation(validIp)
            println(s"Resolved Location for device_id $did: $location")
            val channel = body.request.channel.getOrElse(defaultChannel)
            val deviceSpec = body.request.dspec
            val data = updateDeviceProfile(
                did,
                channel,
                Option(location.countryCode).map(_.trim).filterNot(_.isEmpty),
                Option(location.countryName).map(_.trim).filterNot(_.isEmpty),
                Option(location.stateCode).map(_.trim).filterNot(_.isEmpty),
                Option(location.state).map(_.trim).filterNot(_.isEmpty),
                Option(location.city).map(_.trim).filterNot(_.isEmpty),
                Option(location.stateCustom).map(_.trim).filterNot(_.isEmpty),
                Option(location.stateCodeCustom).map(_.trim).filterNot(_.isEmpty),
                Option(location.districtCustom).map(_.trim).filterNot(_.isEmpty),
                deviceSpec,
                uaspec.map(_.trim).filterNot(_.isEmpty)
            )
        }
        JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
            Map("message" -> s"Device registered successfully")))

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
        PostgresDBUtil.readLocation(query).headOption.getOrElse(new DeviceLocation())
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

    def updateDeviceProfile(did: String, channel: String, countryCode: Option[String], country: Option[String],
                            stateCode: Option[String], state: Option[String], city: Option[String],
                            stateCustom: Option[String], stateCodeCustom: Option[String], districtCustom: Option[String],
                            deviceSpec: Option[Map[String, AnyRef]], uaspec: Option[String]): ResultSet = {

        val uaspecStr = parseUserAgent(uaspec)
        val queryMap: Map[String, Any] = Map("device_id" -> s"'$did'", "channel" -> s"'$channel'",
            "country_code" -> s"'${countryCode.getOrElse("")}'", "country" -> s"'${country.getOrElse("")}'",
            "state_code" -> s"'${stateCode.getOrElse("")}'", "state" -> s"'${state.getOrElse("")}'", "city" -> s"'${city.getOrElse("")}'",
            "state_custom" -> s"'${stateCustom.getOrElse("")}'","state_code_custom" -> s"'${stateCodeCustom.getOrElse("")}'",
            "district_custom" -> s"'${districtCustom.getOrElse("")}'",
            "device_spec" -> deviceSpec.map(x => JSONUtils.serialize(x.mapValues(_.toString))
              .replaceAll("\"", "'")).getOrElse(Map()),
            "uaspec" -> uaspecStr.getOrElse(""), "updated_date" -> DateTime.now(DateTimeZone.UTC).getMillis)

        val finalQueryValues = queryMap.filter {
            m =>
                m._2 match {
                    case s: String => Option(s).exists(_.trim.nonEmpty)
                    case x: Long => Option(x).isDefined
                    case other => other.asInstanceOf[Map[String, String]].nonEmpty
                }
        }
        val query =
            s"""
               |INSERT INTO ${Constants.DEVICE_DB}.${Constants.DEVICE_PROFILE_TABLE}
               | (${finalQueryValues.keys.mkString(",")})
               | VALUES(${finalQueryValues.values.mkString(",")})
           """.stripMargin
        println(query)
        DBUtil.session.execute(query)
    }
}