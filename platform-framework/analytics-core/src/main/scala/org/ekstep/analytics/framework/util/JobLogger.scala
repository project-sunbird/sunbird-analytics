package org.ekstep.analytics.framework.util

import org.apache.log4j.Logger
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.PData
import org.apache.log4j.Level

object JobLogger {

    val jobName: String = "org.ekstep";
    val level = Level.DEBUG
    def info(logger: Logger, msg: String, className:String) {
        logger.info(JSONUtils.serialize(getMeasuredEvent("INFO", msg, null, Map("modelId"->className))))
    }
    def debug(logger: Logger, msg: String, className:String) {
        logger.debug(JSONUtils.serialize(getMeasuredEvent("DEBUG", msg, null, Map("modelId"->className))))
    }
    def error(logger: Logger, msg: String, className:String, exp: Throwable) {
        logger.error(JSONUtils.serialize(getMeasuredEvent("ERROR", msg, exp, Map("modelId"->className))))
    }
    def warn(logger: Logger, msg: String, className:String) {
        logger.warn(JSONUtils.serialize(getMeasuredEvent("WARN", msg, null, Map("modelId"->className))))
    }

    private def getJobLogger(): Logger = {
        val logger = Logger.getLogger(this.jobName);
        println("Logger Name: ", logger.getName)
        logger;
    }

    private def getMeasuredEvent(level: String, msg: String, throwable: Throwable, config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = Map("level" -> level,
            "messhage" -> msg,
            "throwable" -> throwable);
        val mid = "";
        MeasuredEvent("BE_JOB_LOG", System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, None, None, None,
            Context(PData(config.getOrElse("producerId", "").asInstanceOf[String], config.getOrElse("modelId", "TestLogger").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, null, null),
            null,
            MEEdata(measures));
    }
}