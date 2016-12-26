package org.ekstep.analytics.api.util

import org.apache.spark.SparkContext
import org.ekstep.analytics.api.JobRequest
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import akka.actor.Actor
import org.apache.spark.rdd.RDD

object DBUtil {
	
	case class GetJobRequest(requestId: String, clientId: String, sc: SparkContext);
	case class SaveJobRequest(jobRequest: JobRequest, sc: SparkContext);
	
	def getJobRequest(requestId: String, clientKey: String)(implicit sc: SparkContext): JobRequest = {
        val job = sc.cassandraTable[JobRequest](Constants.PLATFORML_DB, Constants.JOB_REQUEST).where("client_key= ?",clientKey).where("request_id=?",requestId).collect
        println("job:", job.isEmpty);
        if (job.isEmpty) null; else job.last;
    }
	
	def getJobRequestList(clientKey: String)(implicit sc: SparkContext): RDD[JobRequest] = {
		sc.cassandraTable[JobRequest](Constants.PLATFORML_DB, Constants.JOB_REQUEST).where("client_key= ?",clientKey);
	}
	
	def saveJobRequest(jobRequest: JobRequest)(implicit sc: SparkContext) = {
		val rdd = sc.makeRDD(Seq(jobRequest))
		rdd.saveToCassandra(Constants.PLATFORML_DB, Constants.JOB_REQUEST)
	}
}

class DBUtil extends Actor {
	import DBUtil._;
	
	def receive = {
		case GetJobRequest(requestId: String, clientId: String, sc: SparkContext) => getJobRequest(requestId, clientId)(sc);
		case SaveJobRequest(jobRequest: JobRequest, sc: SparkContext) => saveJobRequest(jobRequest)(sc);
	}
}