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

/**
 * @author Santhosh
 */
object BatchJobDriver {

    def process[T](config: JobConfig, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {

        JobContext.parallelization = CommonUtil.getParallelization(config);
        if (null == sc) {
            implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model));
            try {
                _process(config, model);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            _process(config, model);
        }
    }

    private def _process[T](config: JobConfig, model: IBatchModel[T])(implicit mf: Manifest[T], sc: SparkContext) {
        val rdd = DataFetcher.fetchBatchData[T](config.search);
        if (config.deviceMapping.nonEmpty && config.deviceMapping.get)
            JobContext.deviceMapping = rdd.map(x => x.asInstanceOf[Event]).filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
        val filterRdd = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
        val output = model.execute(filterRdd, config.modelParams);
        OutputDispatcher.dispatch(config.output, output);
    }
}