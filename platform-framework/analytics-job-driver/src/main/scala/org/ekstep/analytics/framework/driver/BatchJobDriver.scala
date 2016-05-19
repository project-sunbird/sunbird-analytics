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
import org.ekstep.analytics.framework.TelemetryEventV2
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger

/**
 * @author Santhosh
 */
object BatchJobDriver {

    val className = "org.ekstep.analytics.framework.driver.BatchJobDriver"
    def process[T](config: JobConfig, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {

        JobContext.parallelization = CommonUtil.getParallelization(config);
        JobLogger.debug("Setting number of parallelization to " + JobContext.parallelization, className)
        if (null == sc) {
            implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model));
            JobLogger.debug("SparkContext initialized ", className)
            try {
                _process(config, model);
            } finally {
                JobLogger.debug("Closing SparkContext", className)
                CommonUtil.closeSparkContext();
            }
        } else {
            JobLogger.debug("BatchJobDriver: process, SparkContext is not null and starting the job", className)
            _process(config, model);
        }
    }

    private def _process[T](config: JobConfig, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {
        JobLogger.debug("Fetching data with query " + config.search, className)
        val rdd = DataFetcher.fetchBatchData[T](config.search);
        if (config.deviceMapping.nonEmpty && config.deviceMapping.get)
            JobContext.deviceMapping = mf.toString() match {
                case "org.ekstep.analytics.framework.TelemetryEventV2" =>
                    rdd.map(x => x.asInstanceOf[TelemetryEventV2]).filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
                case "org.ekstep.analytics.framework.Event" =>
                    rdd.map(x => x.asInstanceOf[Event]).filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
                case _ => Map()
            }
        JobLogger.debug("Filtering input data using query " + config.filters, className)
        JobLogger.debug("Sorting input data using query " + config.sort, className)
        val filterRdd = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
        JobLogger.info("BatchJobDriver: _process. Started executing the model " + model.getClass.getName, className)
        val output = model.execute(filterRdd, config.modelParams);
        JobLogger.info("Number of events generated : "+output.count, className)
        OutputDispatcher.dispatch(config.output, output);
    }
}