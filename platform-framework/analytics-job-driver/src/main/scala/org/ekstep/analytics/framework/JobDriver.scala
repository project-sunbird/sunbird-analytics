package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.driver.BatchJobDriver
import org.ekstep.analytics.framework.driver.StreamingJobDriver
import org.ekstep.analytics.framework.util.JSONUtils
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException

/**
 * @author Santhosh
 */
object JobDriver  {

    def run[T](t: String, config: String, model: IBatchModel[T])(implicit mf:Manifest[T]) {
        println("### Starting " + t + " batch with config - " + config + " ###");
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
                    throw new Exception("Unknown job type")
            }
        } catch {
            case e: JsonMappingException =>
                Console.err.println("JobDriver:main() - JobConfig parse error", e.getClass.getName, e.getMessage);
                throw e;
            case e: Exception =>
                Console.err.println("JobDriver:main() - Job error", e.getClass.getName, e.getMessage);
                throw e;
        }
        val t2 = System.currentTimeMillis;
        Console.println("## Model run complete - Time taken to compute - " + (t2 - t1) / 1000 + " ##");
        System.exit(0);
    }

}