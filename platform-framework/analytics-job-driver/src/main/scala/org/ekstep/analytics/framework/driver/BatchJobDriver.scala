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

/**
 * @author Santhosh
 */
object BatchJobDriver {

    def process[T](config: JobConfig, model: IBatchModel[T])(implicit mf:Manifest[T]) {

        JobContext.parallelization = CommonUtil.getParallelization(config);
        val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model));
        try {
            val rdd = DataFetcher.fetchBatchData[T](sc, config.search);
            if(config.deviceMapping.get)
                JobContext.deviceMapping = rdd.map(x => x.asInstanceOf[Event]).filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
            val filterRdd = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
            val output = model.execute(sc, filterRdd, config.modelParams);
            OutputDispatcher.dispatch(config.output, output);
        } finally {
            CommonUtil.closeSparkContext(sc);
        }
    }
}