package org.ekstep.analytics.api.util

import java.nio.charset.Charset

import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.joda.time.DateTime

object APILogger {
	def init(jobName: String) = {
		val apiConf = ConfigFactory.load();		
		val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
		ctx.reconfigure();
		if (apiConf.getBoolean("log4j.appender.kafka.enable")) {
			val config = ctx.getConfiguration();
			val property = Property.createProperty("bootstrap.servers", apiConf.getString("log4j.appender.kafka.broker_host"));
			val layout = PatternLayout.createLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN, null, config, null, Charset.defaultCharset(), false, false, null, null)
			val kafkaAppender = KafkaAppender.createAppender(layout, null, "KafkaAppender", false, apiConf.getString("log4j.appender.kafka.topic"), Array(property));
			kafkaAppender.start();
			config.addAppender(kafkaAppender);
			val loggerConfig = config.getLoggers.get("org.ekstep.analytics.api");
			loggerConfig.addAppender(kafkaAppender, null, null)
			ctx.updateLoggers();
		}
	}

	
	private def logger(name: String): Logger = {
		LogManager.getLogger(name);
	}

	def log(msg: String, data: Option[AnyRef] = None, apiName:String="AnalyticsAPI", name: String = "org.ekstep.analytics.api")(implicit className: String) {
		logger(name).info(JSONUtils.serialize(getAccessMeasuredEvent("LOG", "INFO", msg, data, None, apiName)));
	}

	def logMetrics(metricsData: Option[AnyRef] = None, name: String = "org.ekstep.analytics.api.metrics")(implicit className: String) {
		logger(name).info(JSONUtils.serialize(metricsData));
	}

	private def getAccessMeasuredEvent(eid: String, level: String, msg: String, data: Option[AnyRef], status: Option[String] = None, apiName:String="AnalyticsAPI")(implicit className: String): V3Event = {
		val apiConf = ConfigFactory.load();
		val edata = JSONUtils.deserialize[V3EData](JSONUtils.serialize(data))
		val ts = new DateTime().getMillis
		val mid = org.ekstep.analytics.framework.util.CommonUtil.getMessageId(eid, level, ts, None, None);
		val context = V3Context("analytics.api", Option(V3PData("AnalyticsAPI", Option("3.0"), Option(apiName))), "analytics", None, None, None, None)
   	new V3Event(eid, System.currentTimeMillis(), new DateTime().toString(org.ekstep.analytics.framework.util.CommonUtil.df3), "3.0", mid, Actor("", "System"), context, None, edata)
	}
}