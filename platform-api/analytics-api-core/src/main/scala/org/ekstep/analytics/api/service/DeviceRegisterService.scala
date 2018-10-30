package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.{CommonUtil, DBUtil, JSONUtils}
import org.ekstep.analytics.api._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import akka.actor.Actor
import com.google.common.net.InetAddresses
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.PostgresDBUtil
import com.typesafe.config.ConfigFactory
import com.datastax.driver.core.ResultSet
import org.ekstep.analytics.api.util.DeviceLocation
import is.tagomor.woothee.Classifier;

object DeviceRegisterService {
  
    case class RegisterDevice(did: String, ip: String, request: String, uaspec: String)

    def registerDevice(did: String, ip: String, request: String, uaspec: String): String = {
        val config: Config = ConfigFactory.load()
        val tableName = config.getString("postgres.location_table")
        val body = JSONUtils.deserialize[RequestBody](request)
        val validIp = if(ip.startsWith("192")) body.request.ip_addr.getOrElse("") else ip
        if(validIp.nonEmpty) {
            val ipInt = InetAddresses.coerceToInteger(InetAddresses.forString(validIp))
            val psql = "select * from " + tableName +" where ip_from <= " + ipInt + "AND ip_to >= " + ipInt
            val location = PostgresDBUtil.readLocation(psql).headOption.getOrElse(DeviceLocation("", ""))
            val channel = body.request.channel.getOrElse("")
            val dspec = body.request.dspec
            val data = updateDeviceProfile(did, channel, location.state, location.district, dspec, uaspec)
        }
        JSONUtils.serialize(CommonUtil.OK("analytics.device-register", Map("message" -> "Device registered successfully")));
    }
    
    def updateDeviceProfile(did: String, channel: String, state: String, district: String, dspec: Option[Map[String, String]], uaspec: String): ResultSet = {
        val uaspecMap = Classifier.parse(uaspec);
        val finalMap = Map("agent" -> uaspecMap.get("name"), "ver" -> uaspecMap.get("version"), "system" -> uaspecMap.get("os"), "raw" -> uaspec)
        val uaspecStr = JSONUtils.serialize(finalMap).replaceAll("\"", "'")
        val query = if(dspec.isEmpty) {
            "INSERT INTO " + Constants.DEVICE_DB +"."+ Constants.DEVICE_PROFILE_TABLE + " (device_id, channel, state, district, uaspec, updated_date) VALUES('" + did +"','"+ channel +"','"+ state +"','"+ district +"',"+ uaspecStr +","+ DateTime.now(DateTimeZone.UTC).getMillis + ")"
        }
        else {
            val dspecStr = JSONUtils.serialize(dspec.get).replaceAll("\"", "'")
            "INSERT INTO " + Constants.DEVICE_DB +"."+ Constants.DEVICE_PROFILE_TABLE + " (device_id, channel, state, district, uaspec, dspec, updated_date) VALUES('" + did +"','"+ channel +"','"+ state +"','"+ district +"',"+ uaspecStr +","+ dspecStr +","+ DateTime.now(DateTimeZone.UTC).getMillis + ")"
        }
        DBUtil.session.execute(query)
    }
}

class DeviceRegisterService extends Actor {
    import DeviceRegisterService._

    def receive = {
        case RegisterDevice(did: String, ip: String, request: String, uaspec: String) => sender() ! registerDevice(did, ip, request, uaspec)
    }
}