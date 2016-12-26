package org.ekstep.analytics.api.util

import org.apache.spark.SparkContext
import org.ekstep.analytics.api.JobSummary
import com.datastax.spark.connector._

object DBUtil {
	
	def getJob(requestId: String, clientId: String)(implicit sc: SparkContext): JobSummary = {
        val job = sc.cassandraTable[JobSummary]("general_db", "jobs").where("client_id= ?",clientId).where("request_id=?",requestId).collect
        if (job.isEmpty) null; else job.last;
    }
  
}