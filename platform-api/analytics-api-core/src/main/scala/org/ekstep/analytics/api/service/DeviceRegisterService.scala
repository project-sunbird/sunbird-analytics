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

object DeviceRegisterService {
  
    case class RegisterDevice(did: String, ip: String, request: String)

    def registerDevice(did: String, ip: String, request: String): String = {
        val config: Config = ConfigFactory.load()
        val tableName = config.getString("postgres.location_table")
        val body = JSONUtils.deserialize[RequestBody](request)
        if(ip.nonEmpty) {
            val ipInt = InetAddresses.coerceToInteger(InetAddresses.forString(ip))
            val psql = "select * from " + tableName +" where ip_from <= " + ipInt + "AND ip_to >= " + ipInt
            val location = PostgresDBUtil.readLocation(psql).headOption.getOrElse(DeviceLocation("", ""))
            val channel = body.request.channel.getOrElse("")
            val spec = body.request.spec
            val data = updateDeviceProfile(did, channel, location.state, location.district, spec)
        }
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.device-register", Map("message" -> "Device registered successfully")));
    }
    
    def updateDeviceProfile(did: String, channel: String, state: String, district: String, spec: Option[Map[String, String]]): ResultSet = {
        val query = if(spec.isEmpty) {
            "INSERT INTO " + Constants.DEVICE_DB +"."+ Constants.DEVICE_PROFILE_TABLE + " (device_id, channel, state, district, updated_date) VALUES('" + did +"','"+ channel +"','"+ state +"','"+ district +"',"+ DateTime.now(DateTimeZone.UTC).getMillis + ")"
        }
        else {
            val specStr = JSONUtils.serialize(spec.get).replaceAll("\"", "'")
            "INSERT INTO " + Constants.DEVICE_DB +"."+ Constants.DEVICE_PROFILE_TABLE + " (device_id, channel, state, district, spec, updated_date) VALUES('" + did +"','"+ channel +"','"+ state +"','"+ district +"',"+ specStr +","+ DateTime.now(DateTimeZone.UTC).getMillis + ")"
        }
        DBUtil.session.execute(query)
    }
}

class DeviceRegisterService extends Actor {
    import DeviceRegisterService._

    def receive = {
        case RegisterDevice(did: String, ip: String, request: String) => sender() ! registerDevice(did, ip, request)
    }
}