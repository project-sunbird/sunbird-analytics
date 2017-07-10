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
		val channel = apiConf.getString("log4j.appender.kafka.broker_host")
		MeasuredEvent(eid, System.currentTimeMillis(), System.currentTimeMillis(), "1.0", null, "",channel, None, None,
			Context(PData("AnalyticsAPI", "1.0", Option("org.ekstep.analytics.api")), None, "EVENT", null),
			null,MEEdata(data));
	}
}