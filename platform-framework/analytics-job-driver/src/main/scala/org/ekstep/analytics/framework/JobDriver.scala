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
    def run[T](t: String, config: String, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {
        
        JobLogger.init(model.getClass.getSimpleName);
        JobLogger.debug("Starting " + t + " job with config - " + config, className)
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
                    JobLogger.error("Failed Job, JobDriver: main ", className, exp)
                    throw exp
            }
        } catch {
            case e: JsonMappingException =>
                JobLogger.error("JobDriver:main() - JobConfig parse error", className, e)
                throw e;
            case e: Exception =>
                JobLogger.error("JobDriver:main() - Job error", className, e)
                throw e;
        }
        val t2 = System.currentTimeMillis;
        JobLogger.debug("Model run complete - Time taken to compute - " + (t2 - t1) / 1000, className)
    }

}