package org.ekstep.analytics.framework.util

import java.sql.{Connection, Timestamp}

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc._
import javax.inject._
import org.ekstep.analytics.framework.DeviceProfileOutput

@Singleton
class PostgresDBUtil {

    implicit val config: Config = ConfigFactory.load()
    private lazy val db = config.getString("postgres.db")
    private lazy val url = config.getString("postgres.url")
    private lazy val user = config.getString("postgres.user")
    private lazy val pass = config.getString("postgres.pass")

    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(s"$url$db", user, pass)

    implicit val session: AutoSession = AutoSession

    // $COVERAGE-OFF$ cannot be covered since it is dependent on client library
//    def read(sqlString: String): List[ConsumerChannel] = {
//        SQL(sqlString).map(rs => ConsumerChannel(rs)).list().apply()
//    }

    def readLocation(sqlString: String): List[DeviceLocation] = {
        SQL(sqlString).map(rs => DeviceLocation(rs)).list().apply()
    }

    def readGeoLocationCity(sqlString: String): List[GeoLocationCity] = {
        SQL(sqlString).map(rs => GeoLocationCity(rs)).list().apply()
    }

    def readGeoLocationRange(sqlString: String): List[GeoLocationRange] = {
        SQL(sqlString).map(rs => GeoLocationRange(rs)).list().apply()
    }

    def executeQuery(sqlString: String) = {
        SQL(sqlString)
    }

    def readDBData(sqlString: String) = {
        SQL(sqlString).map(rs => DeviceProfileOutput(rs)).list().apply()
    }

    def insertDataToPostgresDB(sqlString: String) = {
        SQL(sqlString).execute().apply()
    }

    def readFirstAccessFromDB(sqlString: String) = {
        SQL(sqlString).map(rs => FirstAccessByDeviceID(rs)).list().apply()
    }

    def checkConnection = {
        try {
            val conn = ConnectionPool.borrow()
            conn match {
                case c: Connection => {
                    conn.close()
                    true
                }
                case _ => false
            }
        } catch {
            case ex: Exception => false
        }
    }
}

case class DeviceLocation(continentName: String, countryCode: String, countryName: String, stateCode: String,
                          state: String, subDivsion2: String, city: String,
                          stateCustom: String, stateCodeCustom: String, districtCustom: String) {
    def this() = this("", "", "", "", "", "", "","","","")

    def toMap() = Map("continent_name" -> continentName,
        "country_code" -> countryCode, "country_name" -> countryName, "state_code" -> stateCode,
        "state" -> state, "city" -> city, "state_custom" -> stateCustom, "state_code_custom" -> stateCodeCustom,
        "district_custom" -> districtCustom)
}

object DeviceLocation extends SQLSyntaxSupport[DeviceLocation] {
    def apply(rs: WrappedResultSet) = new DeviceLocation(
        rs.string("continent_name"),
        rs.string("country_code"),
        rs.string("country_name"),
        rs.string("state_code"),
        rs.string("state"),
        rs.string("sub_div_2"),
        rs.string("city"),
        rs.string("state_custom"),
        rs.string("state_code_custom"),
        rs.string("district_custom")
    )
}

case class GeoLocationCity(geoname_id: Int, subdivision_1_name: String, subdivision_2_custom_name: String) {
    def this() = this(0, "", "")
}

object GeoLocationCity extends SQLSyntaxSupport[GeoLocationCity] {
    def apply(rs: WrappedResultSet) = new GeoLocationCity(
        rs.int("geoname_id"),
        rs.string("subdivision_1_name"),
        rs.string("subdivision_2_custom_name")
    )
}

case class GeoLocationRange(network_start_integer: Long, network_last_integer: Long, geoname_id: Int) {
    def this() = this(0, 0, 0)
}

object GeoLocationRange extends SQLSyntaxSupport[GeoLocationRange] {
    def apply(rs: WrappedResultSet) = new GeoLocationRange(
        rs.long("network_start_integer"),
        rs.long("network_last_integer"),
        rs.int("geoname_id")
    )
}

object DeviceProfileOutput extends SQLSyntaxSupport[DeviceProfileOutput] {
    def apply(rs: WrappedResultSet) = new DeviceProfileOutput(
        rs.string("device_id"),
        rs.timestampOpt("first_access"),
        rs.timestampOpt("last_access"),
        rs.doubleOpt("total_ts"),
        rs.longOpt("total_launches"),
        rs.doubleOpt("avg_ts"),
        rs.stringOpt("device_spec"),
        rs.stringOpt("uaspec"),
        rs.stringOpt("state"),
        rs.stringOpt("city"),
        rs.stringOpt("country"),
        rs.stringOpt("country_code"),
        rs.stringOpt("state_code"),
        rs.stringOpt("state_custom"),
        rs.stringOpt("state_code_custom"),
        rs.stringOpt("district_custom"),
        rs.stringOpt("fcm_token"),
        rs.stringOpt("producer_id"),
        rs.stringOpt("user_declared_state"),
        rs.stringOpt("user_declared_district"),
        rs.timestampOpt("api_last_updated_on"),
        rs.timestampOpt("updated_date")
    )
}

case class FirstAccessByDeviceID(first_access: Option[Timestamp], device_id: String)

object FirstAccessByDeviceID extends SQLSyntaxSupport[FirstAccessByDeviceID] {
    def apply(rs: WrappedResultSet) = new FirstAccessByDeviceID(
        rs.timestampOpt("first_access"),
        rs.string("device_id")
    )
}
// $COVERAGE-ON$