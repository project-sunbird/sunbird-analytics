package org.ekstep.analytics.api.util

import java.nio.charset.Charset

import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.ekstep.analytics.framework.{Context, MEEdata, MeasuredEvent, PData}

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

	
	private def logger(): Logger = {
		LogManager.getLogger("org.ekstep.analytics.api");
	}

	def log(msg: String, data: Option[AnyRef] = None)(implicit className: String) {
		logger.info(JSONUtils.serialize(getAccessMeasuredEvent("BE_ACCESS", "INFO", msg, data)));
	}

	private def getAccessMeasuredEvent(eid: String, level: String, msg: String, data: Option[AnyRef], status: Option[String] = None)(implicit className: String): MeasuredEvent = {
		val mid = "";
		val apiConf = ConfigFactory.load();
		val channel = apiConf.getString("default.channel.id")
		MeasuredEvent(eid, System.currentTimeMillis(), System.currentTimeMillis(), "1.0", null, "",channel, None, None,
			Context(PData("AnalyticsAPI", "1.0", Option("org.ekstep.analytics.api")), None, "EVENT", null),
			null,MEEdata(Option(Map("message" -> msg, "data"-> data.getOrElse(null)))));
	}
}