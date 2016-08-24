package org.ekstep.analytics.api.service

import akka.actor.Actor
import akka.actor.Props

object DataProductManagementAPIService {
	def props = Props[DataProductManagementAPIService];
	case class RunJob(job: String);
	case class ReplyJob(job: String, from: String, to: String);
}

class DataProductManagementAPIService extends Actor {
	import DataProductManagementAPIService._
	def receive = {
		case RunJob(job: String) =>
			println("Run Job started for "+job);
			sender() ! "success";
		
		case ReplyJob(job: String, from: String, to: String) =>
			println("Reply Job started for '"+job+"' from:"+from+" to:"+to);
			sender() ! "success";
	}
}