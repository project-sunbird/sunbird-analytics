package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.joda.time.DateTime
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object DBUtil {
  
    def importContentData(keyspace: String, table: String, file: String, text_delimiter: String = ",")(implicit sc: SparkContext) {
         val rdd = sc.textFile(file)
         rdd.map { x => 
             val values = x.split(text_delimiter)
             val body = values(1).getBytes
             ContentData(values(0), Option(body), Option(new DateTime(System.currentTimeMillis())), Option(body))
         }.saveToCassandra(keyspace, table)
    }
    
    def truncateTable(keyspace: String, table: String)(implicit sc: SparkContext){
        CassandraConnector(sc.getConf).withSessionDo { session =>
             session.execute(s"TRUNCATE $keyspace.$table");   
             session.close()
        }
    }
}