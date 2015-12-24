package org.ekstep.ilimi.analytics.framework.driver

import org.ekstep.ilimi.analytics.framework.DataFetcher
import org.ekstep.ilimi.analytics.framework.DataFilter
import org.ekstep.ilimi.analytics.framework.JobConfig
import org.ekstep.ilimi.analytics.framework.JobRunner
import org.ekstep.ilimi.analytics.framework.OutputDispatcher
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.JobContext
import org.ekstep.ilimi.analytics.framework.Event
import org.ekstep.ilimi.analytics.framework.util.JSONUtils

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