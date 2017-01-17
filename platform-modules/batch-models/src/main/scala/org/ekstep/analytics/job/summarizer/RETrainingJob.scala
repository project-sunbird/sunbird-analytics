package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.Level.INFO
import optional.Application
import org.ekstep.analytics.model.RETrainingModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig

object RETrainingJob extends Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.RETrainingJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        val conf = JSONUtils.deserialize[JobConfig](config)
        val localPath = conf.modelParams.get.getOrElse("localPath", "/tmp/").asInstanceOf[String];
        initLoggingDir(localPath);
        JobLogger.log("Started executing Job", Option(Map("config" -> JSONUtils.deserialize[JobConfig](config))), INFO, "org.ekstep.analytics.model")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, RETrainingModel);
        JobLogger.log("Job Completed.", None, INFO, "org.ekstep.analytics.model")
    }
    
    def initLoggingDir(logPath: String) {
        val dateTime = new DateTime()
        val date = dateTime.toLocalDate()
        val time = dateTime.toLocalTime().toString("hh-mm")
        val path = "/training/" + date + "/" + time + "/"
        val logDir = logPath + path;
        System.setProperty("REDir", logDir);
        val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
        ctx.reconfigure();
    }
}