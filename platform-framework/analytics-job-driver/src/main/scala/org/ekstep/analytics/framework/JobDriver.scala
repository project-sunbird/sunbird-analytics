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

/**
 * @author Santhosh
 */
object JobDriver {

    val logger = Logger.getLogger(JobLogger.jobName)
    val className = this.getClass.getName
    def run[T](t: String, config: String, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {
        println("### Starting " + t + " batch with config - " + config + " ###");
        JobLogger.debug(logger, "Starting " + t + " job with config - " + config, className)
        AppConf.init();
        val t1 = System.currentTimeMillis;
        try {
            val jobConfig = JSONUtils.deserialize[JobConfig](config);
            t match {
                case "batch" =>
                    BatchJobDriver.process[T](jobConfig, model);
                case "streaming" =>
                    StreamingJobDriver.process(jobConfig);
                case _ =>
                    val exp = new Exception("Unknown job type")
                    JobLogger.error(logger, "Failed Job, JobDriver: main ", className, exp)
                    throw exp
            }
        } catch {
            case e: JsonMappingException =>
                Console.err.println("JobDriver:main() - JobConfig parse error", e.getClass.getName, e.getMessage);
                JobLogger.error(logger, "JobDriver:main() - JobConfig parse error", className, e)
                throw e;
            case e: Exception =>
                Console.err.println("JobDriver:main() - Job error", e.getClass.getName, e.getMessage);
                JobLogger.error(logger, "JobDriver:main() - Job error", className, e)
                throw e;
        }
        val t2 = System.currentTimeMillis;
        JobLogger.debug(logger, "Model run complete - Time taken to compute - " + (t2 - t1) / 1000, className)
        Console.println("## Model run complete - Time taken to compute - " + (t2 - t1) / 1000 + " ##");
    }

}