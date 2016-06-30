package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.driver.BatchJobDriver
import org.ekstep.analytics.framework.driver.StreamingJobDriver
import org.ekstep.analytics.framework.util.JSONUtils
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.json4s.JsonUtil
import org.apache.logging.log4j.LogManager

/**
 * @author Santhosh
 */
object JobDriver {

    val className = "org.ekstep.analytics.framework.JobDriver"
    def run[T, R](t: String, config: String, model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        JobLogger.init(model.getClass.getName.split("\\$").last);
        AppConf.init();
        try {
            val jobConfig = JSONUtils.deserialize[JobConfig](config);
            JobLogger.info("Starting " + t + " job with config", className, Option(jobConfig), "BE_JOB_START", Option("START"))
            t match {
                case "batch" =>
                    BatchJobDriver.process[T, R](jobConfig, model);
                case "streaming" =>
                    StreamingJobDriver.process(jobConfig);
                case _ =>
                    val exp = new Exception("Unknown job type")
                    JobLogger.error(exp.getMessage, className, exp, None, "BE_JOB_LOG")
                    throw exp
            }
        } catch {
            case e: JsonMappingException =>
                JobLogger.error(e.getMessage, className, e, None, "BE_JOB_END", Option("FAILED"))
                throw e;
            case e: Exception =>
                JobLogger.error(e.getMessage, className, e, None, "BE_JOB_END", Option("FAILED"))
                throw e;
        }
    }
    
    def run[T, R](t: String, config: String, models: List[IBatchModel[T, R]], jobName: String)(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        JobLogger.init(jobName);
        AppConf.init();
        try {
            val jobConfig = JSONUtils.deserialize[JobConfig](config);
            JobLogger.info("Starting " + t + " jobs with config", className, Option(jobConfig), "BE_JOB_START")
            t match {
                case "batch" =>
                    BatchJobDriver.process[T, R](jobConfig, models);
                case "streaming" =>
                    StreamingJobDriver.process(jobConfig);
                case _ =>
                    val exp = new Exception("Unknown job type")
                    JobLogger.error("Failed Job, JobDriver: main ", className, exp, None, "BE_JOB_LOG")
                    throw exp
            }
        } catch {
            case e: JsonMappingException =>
                JobLogger.error(e.getMessage, className, e, None, "BE_JOB_END", Option("FAILED"))
                throw e;
            case e: Exception =>
                JobLogger.error(e.getMessage, className, e, None, "BE_JOB_END", Option("FAILED"))
                throw e;
        }
    }

}