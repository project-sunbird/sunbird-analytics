package org.ekstep.analytics.api.util

import java.sql._
import com.typesafe.config.Config

object PostgresDBUtil {
  def getPostgresConn()(implicit config: Config): Connection = {
        val url = config.getString("postgres.url");
        val user = config.getString("postgres.user");
        val pass = config.getString("postgres.pass");
        var conn: Connection = null;
        try {
            conn = DriverManager.getConnection(url, user, pass);
            return conn;
        } catch {
            case e1: SQLException =>
                e1.printStackTrace()
            case e2: ClassNotFoundException =>
                e2.printStackTrace();
        }
        return conn;
    }
}