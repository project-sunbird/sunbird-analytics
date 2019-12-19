package org.ekstep.analytics.util

import java.sql.{ResultSet, Statement}

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

object EmbeddedPostgresql {

  var pg: EmbeddedPostgres = null;
  var stmt: Statement = null;

  def start() {
    pg = EmbeddedPostgres.builder().setPort(65124).start()
    val connection = pg.getPostgresDatabase().getConnection()
    stmt = connection.createStatement()
  }

  def execute(sqlString: String): Boolean = {
    stmt.execute(sqlString)
  }

  def executeQuery(sqlString: String): ResultSet = {
    stmt.executeQuery(sqlString)
  }

  def close() {
    pg.close()
  }
}
