package org.ekstep.analytics.util

import java.sql.{ResultSet, Statement}

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.sql.Connection

object EmbeddedPostgresql {

  var pg: EmbeddedPostgres = null;
  var connection: Connection = null;
  var stmt: Statement = null;

  def start() {
    println("***************STARTING POSTGRES")
    pg = EmbeddedPostgres.builder().setPort(65124).start()
    connection = pg.getPostgresDatabase().getConnection()
    stmt = connection.createStatement()
    println(pg.getPort + "   " + pg.getJdbcUrl("postgres", "postgres") + "    " + stmt.getConnection.getMetaData)
  }

  def execute(sqlString: String): Boolean = {
    println("Executing psql query: " + sqlString)
    stmt.execute(sqlString)
  }

  def executeQuery(sqlString: String): ResultSet = {
    stmt.executeQuery(sqlString)
  }

  def close() {
    println("*************CLOSING POSTGRES")
    stmt.close()
    connection.close()
    pg.close()
  }
}
