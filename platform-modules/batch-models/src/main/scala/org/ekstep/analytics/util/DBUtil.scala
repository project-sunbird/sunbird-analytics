package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.joda.time.DateTime
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object DBUtil {

    def truncateTable(keyspace: String, table: String)(implicit sc: SparkContext){
        CassandraConnector(sc.getConf).withSessionDo { session =>
             session.execute(s"TRUNCATE $keyspace.$table");   
             session.close()
        }
    }
}