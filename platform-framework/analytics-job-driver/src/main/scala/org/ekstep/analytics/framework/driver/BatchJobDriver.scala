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
    def process[T, R](config: JobConfig, model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        JobContext.parallelization = CommonUtil.getParallelization(config);
        JobLogger.debug("Setting number of parallelization", className, Option(Map("parallelization" -> JobContext.parallelization)))
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

    def process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        JobContext.parallelization = CommonUtil.getParallelization(config);
        JobLogger.debug("Setting number of parallelization", className, Option(Map("parallelization" -> JobContext.parallelization)))
        if (null == sc) {
            implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model));
            JobLogger.debug("SparkContext initialized", className)
            try {
                _process(config, models);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            JobLogger.debug("SparkContext is not null and starting the job", className)
            _process(config, models);
        }
    }

    private def _process[T, R](config: JobConfig, model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        val t1 = System.currentTimeMillis;
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
        JobLogger.info("Started Executing The Model", className, None, "BE_JOB_LOG")
        val output = model.execute(filterRdd, config.modelParams);
        OutputDispatcher.dispatch(config.output, output);
        JobContext.cleanUpRDDs();
        val date = config.search.queries.get.last.endDate
        val t2 = System.currentTimeMillis;
        JobLogger.info("The model execution completed and generated the output events", className, Option(Map("date" -> date, "events" -> output.count, "timeTaken" -> Double.box((t2 - t2) / 1000))), "BE_JOB_END", Option("COMPLETED"))
    }

    private def _process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
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
        models.foreach { model =>
            val t1 = System.currentTimeMillis;
            JobContext.jobName = model.getClass.getName.split("\\$").last
            JobLogger.info("Started Executing The Model", className, None, "BE_JOB_LOG")
            val output = model.execute(filterRdd, config.modelParams);
            OutputDispatcher.dispatch(config.output, output);
            JobContext.cleanUpRDDs();
            val date = config.search.queries.get.last.endDate
            val t2 = System.currentTimeMillis;
            JobLogger.info("The model execution completed and generated the output events", className, Option(Map("date" -> date, "events" -> output.count, "timeTaken" -> Double.box((t2 - t2) / 1000))), "BE_JOB_END", Option("COMPLETED"))
        }
    }
}