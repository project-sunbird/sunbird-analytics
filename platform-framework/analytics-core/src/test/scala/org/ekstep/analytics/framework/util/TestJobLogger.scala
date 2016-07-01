package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.Level
import org.ekstep.analytics.framework.Level._

class TestJobLogger extends BaseSpec {

    "JobLogger" should "pass test cases for all the methods in JobLogger" in {
        val jobName = "org.ekstep.analytics.framework.util.TestJobLogger"
        JobLogger.init(jobName);

        JobLogger.start("testing start method", jobName, None, Option("START"))
        JobLogger.log("testing info method", jobName, None, None, None, INFO)
        JobLogger.log("testing debug method", jobName, None, None, None)
        JobLogger.log("testing warn method", jobName, None, None, None, WARN)
        JobLogger.log("testing error method", jobName, Option(new Exception), None, Option("FAILED"), ERROR)
        JobLogger.end("testing end method", jobName, None, None, Option("COMPLETED"), INFO)
        JobLogger.end("testing end method", jobName, Option(new Exception), None, Option("FAILED"), ERROR)
        
    }
    
    it should "cover all cases" in {
        val jobName = "org.ekstep.analytics.framework.util.TestJobLogger"
        
        val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
        val config = ctx.getConfiguration();
        val loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.ALL);
        ctx.updateLoggers();

        JobLogger.log("testing info method", jobName, None, None, None,INFO);
        JobLogger.log("testing debug method", jobName,None, None, None, DEBUG);
        JobLogger.log("testing warn method", jobName, None, None, None, WARN);
        JobLogger.log("testing error method", jobName, Option(new Exception), None, Option("FAILED"), ERROR);
        
        loggerConfig.setLevel(Level.OFF);
        ctx.updateLoggers();
        JobLogger.log("testing info method", jobName, None, None, None,INFO);
        JobLogger.log("testing debug method", jobName,None, None, None, DEBUG);
        JobLogger.log("testing warn method", jobName, None, None, None, WARN);
        JobLogger.log("testing error method", jobName, Option(new Exception), None, Option("FAILED"), ERROR);
        
    }

}