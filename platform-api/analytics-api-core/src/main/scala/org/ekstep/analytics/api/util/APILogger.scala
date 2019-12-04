package org.ekstep.analytics.api.util

import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.{LogManager, Logger}
import org.ekstep.analytics.framework._
import org.joda.time.DateTime

object APILogger {
	def init(jobName: String) = {
		val apiConf = ConfigFactory.load()
		val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
		ctx.reconfigure()
	}

	
	private def logger(name: String): Logger = {
		LogManager.getLogger(name)
	}

	def log(msg: String, data: Option[AnyRef] = None, apiName:String="AnalyticsAPI", name: String = "org.ekstep.analytics.api")(implicit className: String) {
		logger(name).info(JSONUtils.serialize(getAccessMeasuredEvent("LOG", "INFO", msg, data, None, apiName)))
	}

	def logMetrics(metricsData: Option[AnyRef] = None, name: String = "org.ekstep.analytics.api.metrics")(implicit className: String) {
		logger(name).info(JSONUtils.serialize(metricsData))
	}

	private def getAccessMeasuredEvent(eid: String, level: String, msg: String, data: Option[AnyRef], status: Option[String] = None, apiName:String="AnalyticsAPI")(implicit className: String): V3Event = {
		val apiConf = ConfigFactory.load()
		val edataMap = data.getOrElse(Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
		val updatedEdataMap = edataMap ++ Map("level" -> level, "message" -> msg, "type" -> "system")
		val edata = JSONUtils.deserialize[V3EData](JSONUtils.serialize(updatedEdataMap))
		val ts = new DateTime().getMillis
		val mid = org.ekstep.analytics.framework.util.CommonUtil.getMessageId(eid, level, ts, None, None)
		val context = V3Context("analytics.api", Option(V3PData("AnalyticsAPI", Option("3.0"), Option(apiName))), "analytics", None, None, None, None)
   	new V3Event(eid, System.currentTimeMillis(), new DateTime().toString(org.ekstep.analytics.framework.util.CommonUtil.df3), "3.0", mid, Actor("", "System"), context, None, edata, List())
	}
}