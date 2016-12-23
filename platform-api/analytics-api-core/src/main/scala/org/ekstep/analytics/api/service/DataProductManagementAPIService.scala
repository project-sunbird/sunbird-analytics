package org.ekstep.analytics.api.service

import akka.actor.Actor
import akka.actor.Props
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import com.typesafe.config.Config
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.commons.lang.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.RequestBody

object DataProductManagementAPIService {
	def props = Props[DataProductManagementAPIService];
	case class RunJob(job: String, body: String, config: Config);
	case class ReplayJob(job: String, from: String, to: String, body: String, config: Config);
}

class DataProductManagementAPIService extends Actor {
	import DataProductManagementAPIService._
	def receive = {
		// $COVERAGE-OFF$ Disabling scoverage - because actor calls are Async.
		case RunJob(job: String, body: String, config: Config) =>
			println("Run Job started for "+job);
			val script = config.getString("dataproduct.scripts_path") + "/run-job.sh "+job;
			val scriptParams = Map(
					"PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"),
					"script" -> script,
					"job_config" -> getConfig(body),
					"aws_key" -> AppConf.getAwsKey(),
                	"aws_secret" -> AppConf.getAwsSecret());
            ScriptDispatcher.dispatch(Array[String](), scriptParams).foreach(println);
            println("Run Job completed for "+job);
			sender() ! "success";
		
		case ReplayJob(job: String, from: String, to: String, body: String, config: Config) =>
			println("Reply Job started for '"+job+"' from:"+from+" to:"+to);
			val script = config.getString("dataproduct.scripts_path") + "/replay-job.sh "+job+" "+from+" "+to;
			val scriptParams = Map(
					"PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"),
					"script" -> script,
					"job_config" -> getConfig(body),
					"aws_key" -> AppConf.getAwsKey(),
                	"aws_secret" -> AppConf.getAwsSecret());
            ScriptDispatcher.dispatch(Array[String](), scriptParams).foreach(println);
            println("Reply Job completed for '"+job+"' from:"+from+" to:"+to);
			sender() ! "success";
		// $COVERAGE-ON$
	}
	
	private def getConfig(body: String): String = {
		val reqBody = JSONUtils.deserialize[RequestBody](body);
		if (null == reqBody.request || reqBody.request.config.isEmpty) {
			"";
		} else {
			JSONUtils.serialize(reqBody.request.config.get);
		}
	}
}