package org.ekstep.analytics.api.util

import java.sql._
import com.typesafe.config.Config

object PostgresDBUtil {

    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null

    def getConn(url: String, user: String, pass: String)(implicit config: Config): Connection = {

        try {
            conn = DriverManager.getConnection(url, user, pass);
        } catch {
            case e1: SQLException =>
                e1.printStackTrace()
            case e2: ClassNotFoundException =>
                e2.printStackTrace()
        }
        return conn
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
            if(rs!=null) rs.close()
            if(stmt!=null) stmt.close()
        }catch {
            case e: Throwable => e.printStackTrace()
        }
    }

    def execute(conn: Connection, sql: String): ResultSet = {
        try {
            if (conn != null) {
                stmt = conn.createStatement()
                rs = stmt.executeQuery(sql)
            }
        } catch {
            case t: Throwable => t.printStackTrace()
        }
        return rs
    }
}