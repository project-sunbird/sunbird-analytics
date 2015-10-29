package org.ekstep.ilimi.analytics.framework

import org.ekstep.ilimi.analytics.framework.util.Application
import org.ekstep.ilimi.analytics.framework.conf.AppConf
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.driver.BatchJobDriver
import org.ekstep.ilimi.analytics.framework.driver.StreamingJobDriver


/**
 * @author Santhosh
 */
object JobDriver extends Application {

    def main(t: String, config: String) {

        AppConf.init();
        val t1 = System.currentTimeMillis;
        try {
            val jobConfig = CommonUtil.getJobConfig(config);
            t match {
                case "batch"     =>
                    BatchJobDriver.process(jobConfig);
                case "streaming" =>
                    StreamingJobDriver.process(jobConfig);
                case _           =>
                    throw new Exception("Unknown job type")
            }
        } catch {
            case e: Exception =>
                Console.err.println("JobDriver Error", e.getClass.getName, e.getMessage);
                e.printStackTrace();
        }
        val t2 = System.currentTimeMillis;
        Console.println("## Model run complete - Time taken to compute - " + (t2 - t1) / 1000 + " ##");
    }

}