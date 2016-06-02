package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.Level

class TestJobLogger extends BaseSpec {

    "JobLogger" should "pass test cases for all the methods in JobLogger" in {
        val jobName = "org.ekstep.analytics.framework.util.TestJobLogger"
        JobLogger.init(jobName);

        JobLogger.info("testing info method", jobName);
        JobLogger.debug("testing debug method", jobName);
        JobLogger.warn("testing warn method", jobName);
        JobLogger.error("testing error method", jobName, new Exception);
        
    }
    
    it should "cover all cases" in {
        val jobName = "org.ekstep.analytics.framework.util.TestJobLogger"
        
        val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
        val config = ctx.getConfiguration();
        val loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.ALL);
        ctx.updateLoggers();

        JobLogger.info("testing info method", jobName);
        JobLogger.debug("testing debug method", jobName);
        JobLogger.warn("testing warn method", jobName);
        JobLogger.error("testing error method", jobName, new Exception);
        
        loggerConfig.setLevel(Level.OFF);
        ctx.updateLoggers();
        JobLogger.info("testing info method", jobName);
        JobLogger.debug("testing debug method", jobName);
        JobLogger.warn("testing warn method", jobName);
        JobLogger.error("testing error method", jobName, new Exception);
        
    }

}