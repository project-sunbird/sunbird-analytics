package org.ekstep.analytics.api.util

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import scalikejdbc._
import javax.inject._

case class TestData(ID: String, NAME: String)

@Singleton
class H2DBUtil {

    private val DB_DRIVER = "org.h2.Driver"
    private val DB_CONNECTION = "jdbc:h2:mem:test;MODE=MYSQL"
    private val DB_USER = ""
    private val DB_PASSWORD = ""

    val connection = getDBConnection();

    def getDBConnection(): Connection = {
        var dbConnection: Connection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch {
            case e: ClassNotFoundException =>
                System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
        } catch {
            case e: SQLException =>
                System.out.println(e.getMessage());
        }
        return dbConnection;
    }

    def readLocation(sqlString: String): DeviceStateDistrict = {
        val resultSet = connection.prepareStatement(sqlString).executeQuery()
        var loc = new DeviceStateDistrict();
        while (resultSet.next()) {
            loc =  DeviceStateDistrict(resultSet)
        }
        loc
    }

    def executeQuery(sqlString: String) = {
        connection.prepareStatement(sqlString).execute()
    }

    def execute(sqlString: String): ResultSet = {
        val resultSet = connection.prepareStatement(sqlString).executeQuery()
        resultSet
    }

}

case class DeviceStateDistrict(state: String, districtCustom: String) {
    def this() = this("", "")
}

object DeviceStateDistrict extends SQLSyntaxSupport[DeviceStateDistrict] {
    def apply(rs: ResultSet) = new DeviceStateDistrict(
        rs.getString("state"),
        rs.getString("district_custom")
    )
}