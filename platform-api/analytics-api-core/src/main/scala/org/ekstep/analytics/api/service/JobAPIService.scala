package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.CommonUtil
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.JobStatusResponse
import org.ekstep.analytics.api.JobOutput
import java.util.UUID
import org.ekstep.analytics.api.JobStatus
import org.ekstep.analytics.framework.util.JSONUtils
import scala.util.Random
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.api.JobStats


/**
 * @author mahesh
 */

object JobAPIService {
	
	def dataExhaust()(implicit sc: SparkContext): String = {
		val result = randomJobStatus(None)
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.data.out", result));	
	}
	
	def getJob(clientId: String, jobId: String)(implicit sc: SparkContext): String = {
		val result = randomJobStatus(Option(jobId))
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.job.info", result));	
	}
	
	def getJobList(clientId: String)(implicit sc: SparkContext): String = {
		val count = Random.nextInt(10)
		val jobs = if (count == 0) {
			List();
		} else {
			val result = for (i <- 1 to count) yield {
				randomJobStatus(None)
			};
			result.toList
		}
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.job.list", Map("count" -> Int.box(jobs.length), "jobs" -> jobs)));	
	}
	
	private def randomJobStatus(jobId : Option[String]) : Map[String, AnyRef] = {
		val id = if (jobId.isEmpty) UUID.randomUUID().toString() else jobId.get;
		val randomNum = Random.nextInt(1000)
		val jobStatus =  if (randomNum %2 == 0) {
			val status = JobStatus.COMPLETED.toString()
			val diff = 1728000;
			val current = CommonUtil.getMillis();
			val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
			val output = JobOutput("https://s3-ap-southeast-1.amazonaws.com/ekstep-public/data-out/1454996593287.zip", df.print(current), current-diff, current, current+diff);
			val stats = JobStats(current-3600, current-2400, current-3000, Random.nextInt(10000), Random.nextInt(2000), Random.nextInt(200), Random.nextInt(1200))
			JobStatusResponse(id, status, CommonUtil.getMillis, Map(), Option(output), Option(stats))
		} else {
			val status = JobStatus.PROCESSING.toString();
			JobStatusResponse(id, status, CommonUtil.getMillis, Map())
		}
		JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(jobStatus))
	}  
}