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

object DeviceRegisterService {
  
    case class RegisterDevice(did: String, request: String)

    def registerDevice(did: String, request: String): String = {
        val config: Config = ConfigFactory.load()
        val tableName = config.getString("postgres.loaction_table")
        val body = JSONUtils.deserialize[RequestBody](request)
        val ip = body.request.ip_addr.getOrElse("")
        val map = if(ip.nonEmpty) {
            val ipInt = InetAddresses.coerceToInteger(InetAddresses.forString(ip))
            val psql = "select * from " + tableName +" where ip_from <= " + ipInt + "AND ip_to >= " + ipInt
            PostgresDBUtil.execute(psql).headOption.getOrElse(Map[String, Any]())
        } else {
          Map[String, Any]();
        }
        val state = map.get("region_name").getOrElse(body.request.state.getOrElse("")).asInstanceOf[String]
        val district = map.get("city_name").getOrElse(body.request.district.getOrElse("")).asInstanceOf[String]
        val channel = body.request.channel.getOrElse("")
        val query = "INSERT INTO " + Constants.DEVICE_DB +"."+ Constants.DEVICE_PROFILE_TABLE + " (device_id, channel, state, district, updated_date) VALUES('" + did +"','"+ channel +"','"+ state +"','"+ district +"',"+ DateTime.now(DateTimeZone.UTC).getMillis + ")"
        val data = DBUtil.session.execute(query)
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.device-register", Map("message" -> "Device registered successfully")));
    }
}

class DeviceRegisterService extends Actor {
    import DeviceRegisterService._

    def receive = {
        case RegisterDevice(did: String, request: String) => sender() ! registerDevice(did, request)
    }
}