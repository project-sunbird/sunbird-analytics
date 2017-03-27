package org.ekstep.analytics.job

import org.ekstep.analytics.framework.IGraphExecutionModelTemplate
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.Level._

/**
 * @author mahesh
 */

trait IGraphExecutionModel extends IGraphExecutionModelTemplate with optional.Application with IJob {

	implicit val className = "org.ekstep.analytics.job.IGraphExecutionModel"
	override def name() : String = "GraphExecutionModel";
	
	def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.init(name)
        JobLogger.start("Started processing of " + name, Option(Map("config" -> config)));
        val jobConfig = JSONUtils.deserialize[JobConfig](config);
        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model));
            try {            	
            	_execute(jobConfig);
            } catch {
                case ex: Exception =>
                    JobLogger.log(ex.getMessage, None, ERROR);
                    JobLogger.end(name + " processing failed", "FAILED",  Option(Map( "timeTaken" -> Double.box(0))));
                    ex.printStackTrace();
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            _execute(jobConfig);
        }
    }
	
	private def _execute(jobConfig: JobConfig)(implicit sparkContext: SparkContext) = {
		val input = sparkContext.emptyRDD[String];
        execute(input, jobConfig.modelParams);
        JobContext.cleanUpRDDs();
        JobLogger.end(name + " processing complete", "SUCCESS", Option(Map( "timeTaken" -> Double.box(0))));
	}
}