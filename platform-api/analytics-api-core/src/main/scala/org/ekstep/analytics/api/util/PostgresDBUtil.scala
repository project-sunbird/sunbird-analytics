package org.ekstep.analytics.api.util

import java.sql._

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc._

object PostgresDBUtil {

    implicit val config: Config = ConfigFactory.load()
    private lazy val url = config.getString("postgres.url")
    private lazy val user = config.getString("postgres.user")
    private lazy val pass = config.getString("postgres.pass")

    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(url, user, pass)

    implicit val session = AutoSession

    /*
    def getConn()(implicit config: Config): Connection = {
        val url = config.getString("postgres.url")
        val user = config.getString("postgres.user")
        val pass = config.getString("postgres.pass")
        DriverManager.getConnection(url, user, pass)
    }

    def closeConn(conn: Connection) {
        try {
            if (conn != null)
                conn.close
        } catch {
            case t: Throwable => t.printStackTrace()
        }
    }

    def closeStmt() {
        try {
            if (rs != null) rs.close()
            if (stmt != null) stmt.close()
        } catch {
            case e: Throwable => e.printStackTrace()
        }
    }
    */

    def read(sqlString: String): List[ConsumerChannel] = {
        sql"""$sqlString""".map(rs => ConsumerChannel(rs)).list().apply()
    }

    def executeQuery(sqlString: String) = {
        sql"""$sqlString"""
    }

    /*
    def execute(sql: String): ResultSet = {
        try {
            val conn = getConn()
            stmt = conn.createStatement()
            rs = stmt.executeQuery(sql)
        } catch {
            case t: Throwable => t.printStackTrace()
        }
        rs
    }
    */
}