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

/**
 * @author Santhosh
 */
object BatchJobDriver {

    def process(config: JobConfig) {

        JobContext.parallelization = CommonUtil.getParallelization(config);
        val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model));
        try {
            val rdd = DataFetcher.fetchBatchData[Event](sc, config.search);
            JobContext.deviceMapping = rdd.filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
            val filterRdd = DataFilter.filterAndSort[Event](rdd, config.filters, config.sort);
            val output = JobRunner.executeBatch(config.model, sc, filterRdd, config.modelParams);
            OutputDispatcher.dispatch(config.output, output);
        } finally {
            CommonUtil.closeSparkContext(sc);
        }
    }
}