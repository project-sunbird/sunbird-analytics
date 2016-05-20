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
        LogManager.getLogger(JobContext.jobName);
    }

    def info(msg: String, className: String) {
        if (logger.isInfoEnabled()) {
            logger.info(JSONUtils.serialize(getMeasuredEvent("INFO", msg, null, Map("modelId" -> className))));
        }
    }

    def debug(msg: String, className: String) {
        if (logger.isDebugEnabled()) {
            logger.debug(JSONUtils.serialize(getMeasuredEvent("DEBUG", msg, null, Map("modelId" -> className))))
        }
    }

    def error(msg: String, className: String, exp: Throwable) {
        if (logger.isErrorEnabled())
            logger.error(JSONUtils.serialize(getMeasuredEvent("ERROR", msg, exp, Map("modelId" -> className))));
    }

    def warn(msg: String, className: String) {
        if (logger.isWarnEnabled())
            logger.warn(JSONUtils.serialize(getMeasuredEvent("WARN", msg, null, Map("modelId" -> className))));
    }

    private def getMeasuredEvent(level: String, msg: String, throwable: Throwable, config: Map[String, String]): MeasuredEvent = {
        val measures = Map(
            "class" -> config.get("modelId"),
            "level" -> level,
            "message" -> msg,
            "throwable" -> throwable);
        val mid = "";
        MeasuredEvent(None, "BE_JOB_LOG", System.currentTimeMillis(), System.currentTimeMillis(), "1.0", null, None, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], JobContext.jobName, config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, null, null),
            null,
            MEEdata(measures));
    }
}