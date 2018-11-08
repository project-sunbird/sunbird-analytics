package org.ekstep.analytics.api.util

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc._

object PostgresDBUtil {

    implicit val config: Config = ConfigFactory.load()
    private lazy val db = config.getString("postgres.db")
    private lazy val url = config.getString("postgres.url")
    private lazy val user = config.getString("postgres.user")
    private lazy val pass = config.getString("postgres.pass")

    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(s"$url$db", user, pass)

    implicit val session: AutoSession = AutoSession

    def read(sqlString: String): List[ConsumerChannel] = {
        SQL(sqlString).map(rs => ConsumerChannel(rs)).list().apply()
    }
    
    def readLocation(sqlString: String): List[DeviceLocation] = {
        SQL(sqlString).map(rs => DeviceLocation(rs)).list().apply()
    }

    def executeQuery(sqlString: String) = {
        SQL(sqlString)
    }
}

case class DeviceLocation(continentName: String, countryName: String, state: String, subDivsion2: String, city: String) {
    def this() = this("", "", "", "", "")
}

object DeviceLocation extends SQLSyntaxSupport[DeviceLocation] {
    def apply(rs: WrappedResultSet) = new DeviceLocation(
        rs.string("continent_name"),
        rs.string("country_name"),
        rs.string("state"),
        rs.string("sub_div_2"),
        rs.string("city")
    )
}