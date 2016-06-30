package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.PData
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext
import org.ekstep.analytics.framework.JobContext

object JobLogger {

    def init(jobName: String) = {
        System.setProperty("logFilename", jobName.toLowerCase());
        val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
        ctx.reconfigure();
        JobContext.jobName = jobName;
    }

    private def logger(): Logger = {
        LogManager.getLogger("org.ekstep.analytics.logger");
    }

    def info(msg: String, className: String, data: Option[AnyRef] = None, eid : String, logType: Option[String] = None) {
        logger.info(JSONUtils.serialize(getMeasuredEvent(eid,"INFO", msg, null, Map("modelId" -> className), data, logType)));
    }

    def debug(msg: String, className: String, data: Option[AnyRef] = None) {
        logger.debug(JSONUtils.serialize(getMeasuredEvent("BE_JOB_LOG_DEBUG","DEBUG", msg, null, Map("modelId" -> className), data)))
    }

    def error(msg: String, className: String, exp: Throwable, data: Option[AnyRef] = None, eid : String, logType: Option[String] = None) {
        logger.error(JSONUtils.serialize(getMeasuredEvent(eid, "ERROR", msg, exp, Map("modelId" -> className), data, logType)));
    }

    def warn(msg: String, className: String, data: Option[AnyRef] = None) {
        logger.warn(JSONUtils.serialize(getMeasuredEvent("BE_JOB_LOG_WARN", "WARN", msg, null, Map("modelId" -> className), data)));
    }

    private def getMeasuredEvent(eid: String, level: String, msg: String, throwable: Throwable, config: Map[String, String], data: Option[AnyRef], logType: Option[String] = None): MeasuredEvent = {
        val measures = Map(
            "class" -> config.get("modelId"),
            "level" -> level,
            "message" -> msg,
            "throwable" -> throwable,
            "status" -> logType,
            "data" -> data);
        val mid = "";
        MeasuredEvent(eid, System.currentTimeMillis(), System.currentTimeMillis(), "1.0", null, "", None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], JobContext.jobName, config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, null, null),
            null,
            MEEdata(measures));
    }
}