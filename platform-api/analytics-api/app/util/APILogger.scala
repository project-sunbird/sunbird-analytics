package util

import java.nio.charset.Charset

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData

import com.typesafe.config.ConfigFactory

object APILogger {
	def init(jobName: String) = {
		val apiConf = play.Play.application.configuration.underlying()
			.withFallback(ConfigFactory.parseMap(Map(
				"log4j.appender.kafka.enable" -> "false",
				"log4j.appender.kafka.broker_host" -> "",
				"log4j.appender.kafka.topic" -> "").asJava))
		val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
		ctx.reconfigure();
		if (StringUtils.equalsIgnoreCase(apiConf.getString("log4j.appender.kafka.enable"), "true")) {
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
		MeasuredEvent(eid, System.currentTimeMillis(), System.currentTimeMillis(), "1.0", null, "", None, None,
			Context(PData("AnalyticsAPI", "org.ekstep.analytics.api", "1.0"), None, "EVENT", null),
			null,
			MEEdata(data));
	}
}