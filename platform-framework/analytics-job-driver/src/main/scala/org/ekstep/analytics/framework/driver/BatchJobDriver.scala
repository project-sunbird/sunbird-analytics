package org.ekstep.analytics.framework.driver

import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.JobRunner
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.IBatchModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger

/**
 * @author Santhosh
 */
object BatchJobDriver {

    val className = "org.ekstep.analytics.framework.driver.BatchJobDriver"
    def process[T](config: JobConfig, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {

        JobContext.parallelization = CommonUtil.getParallelization(config);
        JobLogger.debug("Setting number of parallelization", className, Option(Map("parallelization"->JobContext.parallelization)))
        if (null == sc) {
            implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model));
            JobLogger.debug("SparkContext initialized", className)
            try {
                _process(config, model);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            JobLogger.debug("SparkContext is not null and starting the job", className)
            _process(config, model);
        }
    }

    private def _process[T](config: JobConfig, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {
        JobLogger.debug("Fetching data", className, Option(Map("query" -> config.search)))
        val rdd = DataFetcher.fetchBatchData[T](config.search);
        if (config.deviceMapping.nonEmpty && config.deviceMapping.get)
            JobContext.deviceMapping = mf.toString() match {
                case "org.ekstep.analytics.framework.Event" =>
                    rdd.map(x => x.asInstanceOf[Event]).filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
                case _ => Map()
            }
        JobLogger.debug("Filtering input data", className, Option(Map("query" -> config.filters)))
        JobLogger.debug("Sorting input data", className, Option(Map("query" -> config.sort)))
        val filterRdd = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
        JobLogger.info("Started Executing The Model", className, Option(model.getClass.getName))
        val output = model.execute(filterRdd, config.modelParams);
        JobLogger.info("The Model Execution Completed", className, Option(model.getClass.getName))
        JobLogger.info("Number of events generated", className,Option(Map("events" -> output.count)))
        OutputDispatcher.dispatch(config.output, output);
    }
}