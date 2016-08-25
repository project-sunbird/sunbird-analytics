package org.ekstep.analytics.api.service

import akka.actor.Actor
import akka.actor.Props
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import com.typesafe.config.Config
import org.ekstep.analytics.framework.conf.AppConf

object DataProductManagementAPIService {
	def props = Props[DataProductManagementAPIService];
	case class RunJob(job: String, config: Config);
	case class ReplyJob(job: String, from: String, to: String, config: Config);
}

class DataProductManagementAPIService extends Actor {
	import DataProductManagementAPIService._
	def receive = {
		// $COVERAGE-OFF$ Disabling scoverage - because actor calls are Async.
		case RunJob(job: String, config: Config) =>
			println("Run Job started for "+job);
			val script = config.getString("dataproduct.scripts_path") + "/run-job.sh "+job;
			val scriptParams = Map(
					"PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"),
					"script" -> script,
					"aws_key" -> AppConf.getAwsKey(),
                	"aws_secret" -> AppConf.getAwsSecret());
            ScriptDispatcher.dispatch(Array(), scriptParams).foreach(println);
            println("Run Job completed for "+job);
			sender() ! "success";
		
		case ReplyJob(job: String, from: String, to: String, config: Config) =>
			println("Reply Job started for '"+job+"' from:"+from+" to:"+to);
			val script = config.getString("dataproduct.scripts_path") + "/replay-job.sh "+job+" "+from+" "+to;
			val scriptParams = Map(
					"PATH" -> (sys.env.getOrElse("PATH", "/usr/bin") + ":/usr/local/bin"),
					"script" -> script,
					"aws_key" -> AppConf.getAwsKey(),
                	"aws_secret" -> AppConf.getAwsSecret());
            ScriptDispatcher.dispatch(Array(), scriptParams).foreach(println);
            println("Reply Job completed for '"+job+"' from:"+from+" to:"+to);
			sender() ! "success";
		// $COVERAGE-ON$
	}	
}