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
import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
object BatchJobDriver {

    val className = "org.ekstep.analytics.framework.driver.BatchJobDriver"
    def process[T, R](config: JobConfig, model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        process(config, List(model));
    }

    def process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        JobContext.parallelization = CommonUtil.getParallelization(config);
        if (null == sc) {
            implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model));
            try {
                _process(config, models);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            _process(config, models);
        }
    }

    private def _process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {

        val rdd = DataFetcher.fetchBatchData[T](config.search);
        _setDeviceMapping(config, rdd);
        val data = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
        models.foreach { model =>
            _processModel(config, data, model);
        }
    }

    private def _setDeviceMapping[T](config: JobConfig, data: RDD[T])(implicit mf: Manifest[T]) {
        if (config.deviceMapping.nonEmpty && config.deviceMapping.get) {
            JobContext.deviceMapping = mf.toString() match {
                case "org.ekstep.analytics.framework.Event" =>
                    data.map(x => x.asInstanceOf[Event]).filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
                case _ => Map()
            }
        }
    }

    private def _processModel[T, R](config: JobConfig, data: RDD[T], model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        JobLogger.debug("BatchJobDriver:process() - Started processing of " + model.name, className);
        val result = CommonUtil.time({
            JobContext.jobName = model.name;
            val output = model.execute(data, config.modelParams);
            val count = OutputDispatcher.dispatch(config.output, output);
            JobContext.cleanUpRDDs();
            count;
        })
        JobLogger.info("The model execution completed and generated the output events", className, Option(Map("date" -> config.search.queries.get.last.endDate, "events" -> result._2, "timeTaken" -> Double.box(result._1 / 1000))), "BE_JOB_END", Option("COMPLETED"))
    }
}