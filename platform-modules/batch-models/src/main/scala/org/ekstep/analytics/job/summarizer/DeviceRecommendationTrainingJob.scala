package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.Level.INFO
import optional.Application
import org.ekstep.analytics.model.DeviceRecommendationTrainingModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig

object DeviceRecommendationTrainingJob extends Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.DeviceRecommendationTrainingJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        val conf = JSONUtils.deserialize[JobConfig](config)
        val localPath = conf.modelParams.get.getOrElse("localPath", "/tmp/").asInstanceOf[String];
        val dataTimeFolderStructure = conf.modelParams.get.getOrElse("dataTimeFolderStructure", true).asInstanceOf[Boolean];
        initLoggingDir(localPath, dataTimeFolderStructure);
        JobLogger.log("Started executing Job", Option(Map("config" -> JSONUtils.deserialize[JobConfig](config))), INFO, "org.ekstep.analytics.model")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, DeviceRecommendationTrainingModel);
        JobLogger.log("Job Completed.", None, INFO, "org.ekstep.analytics.model")
    }
    
    def initLoggingDir(logPath: String, dataTimeFolderStructure: Boolean) {
        val dateTime = new DateTime()
        val date = dateTime.toLocalDate()
        val time = dateTime.toLocalTime().toString("HH-mm")
        val path_default = "/training/" + date + "/" + time + "/"
        val path = if(dataTimeFolderStructure) path_default else ""
        val logDir = logPath + path;
        System.setProperty("REDir", logDir);
        val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
        ctx.reconfigure();
    }
}