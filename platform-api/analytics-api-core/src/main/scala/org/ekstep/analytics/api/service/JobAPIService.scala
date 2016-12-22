package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.CommonUtil
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.JobStatusResponse
import org.ekstep.analytics.api.JobOutput
import java.util.UUID
import org.ekstep.analytics.api.JobStatus
import org.ekstep.analytics.framework.util.JSONUtils
import scala.util.Random
import akka.actor.Props
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.api.JobStats
import org.ekstep.analytics.api.JobSummary
import org.ekstep.analytics.streaming.KafkaEventProducer
import org.ekstep.analytics.api.JobRequestEvent
import org.ekstep.analytics.api.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.framework.JobConfig
import org.joda.time.DateTime
import org.ekstep.analytics.framework.Fetcher
import com.typesafe.config.Config

/**
 * @author mahesh
 */

object JobAPIService {

    def props = Props[ContentAPIService];

    def getJobStatusResponse(job: JobSummary): JobStatusResponse = {
        val output = JobOutput(job.locations.getOrElse(List("")), job.file_size.getOrElse(0L), job.dt_file_created.getOrElse(null), job.dt_first_event.getOrElse(null), job.dt_last_event.getOrElse(null), job.dt_expiration.getOrElse(null));
        val stats = JobStats(job.dt_job_submitted.getOrElse(null), job.dt_job_processing.getOrElse(null), job.dt_job_completed.getOrElse(null), job.input_events.getOrElse(0), job.output_events.getOrElse(0), job.latency.getOrElse(0), job.execution_time.getOrElse(0L))
        JobStatusResponse(job.request_id.get, job.status.get, CommonUtil.getMillis, JSONUtils.deserialize[Map[String, AnyRef]](job.request_data.getOrElse(null)), Option(output), Option(stats))
    }

    def checkTheJob(requestId: String, clientId: String)(implicit sc: SparkContext): JobSummary = {
        val job = sc.cassandraTable[JobSummary]("general_db", "jobs").where("client_id= ?",clientId).where("request_id=?",requestId).collect
        if (job.isEmpty) null; else job.last;
    }

    def checkTheJobs(clientId: String, limit: Int)(implicit sc: SparkContext): List[JobSummary] = {
        val job = sc.cassandraTable[JobSummary]("general_db", "jobs").where("client_id=?", clientId).collect.take(limit)
        if (job.isEmpty) null; else job.toList;
    }

    private def writeEventToDb(event: JobRequestEvent, requestData: String)(implicit sc: SparkContext) {
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val request_id = eksMap.get("request_id").get.asInstanceOf[String]
        val job_id = eksMap.get("job_id").get.asInstanceOf[String]
        val config = eksMap.get("config").get.asInstanceOf[JobConfig]
        sc.parallelize(Array(Tuple8(request_id, event.context.client_id.get, job_id, JobStatus.SUBMITTED.toString, requestData, JSONUtils.serialize(config), 1, System.currentTimeMillis()))).saveToCassandra("general_db", "jobs", SomeColumns("request_id", "client_id", "job_id", "status", "request_data", "config", "iteration", "dt_job_submitted"))
    }

    def dataExhaust(event: JobRequestEvent, request: RequestBody, brokerList: String, topic: String)(implicit sc: SparkContext, config: Config): String = {
        if ("true".equals(config.getString("dataExhaust.job.kafka_push"))) {
            KafkaEventProducer.sendEvent(event, topic, brokerList)
        }
        writeEventToDb(event, JSONUtils.serialize(request.request))
        JSONUtils.serialize(event);
    }

    def getJob(clientId: String, requestId: String)(implicit sc: SparkContext): String = {
        val job = checkTheJob(requestId, clientId)(sc)
        val jobStatusRes = getJobStatusResponse(job)
        val result = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(jobStatusRes))
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.job.info", result));
    }

    def getJobList(clientId: String, limit: Int = 100)(implicit sc: SparkContext): String = {
        val listJobs = checkTheJobs(clientId, limit)
        val result = listJobs.map { x => getJobStatusResponse(x) }
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.job.list", Map("count" -> Int.box(result.length), "jobs" -> result)));
    }

    private def randomJobStatus(jobId: Option[String]): Map[String, AnyRef] = {
        val id = if (jobId.isEmpty) UUID.randomUUID().toString() else jobId.get;
        val randomNum = Random.nextInt(1000)
        val jobStatus = if (randomNum % 2 == 0) {
            val status = JobStatus.COMPLETED.toString()
            val diff = 1728000;
            val current = CommonUtil.getMillis();
            val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
            //			val output = JobOutput(List("https://s3-ap-southeast-1.amazonaws.com/ekstep-public/data-out/1454996593287.zip"), 120, df.print(current), current-diff, current, current+diff);
            //			val stats = JobStats(current-3600, current-2400, current-3000, Random.nextInt(10000), Random.nextInt(2000), Random.nextInt(200), Random.nextInt(1200))
            JobStatusResponse(id, status, CommonUtil.getMillis, Map(), Option(null), Option(null))
        } else {
            val status = JobStatus.PROCESSING.toString();
            JobStatusResponse(id, status, CommonUtil.getMillis, Map())
        }
        JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(jobStatus))
    }
}