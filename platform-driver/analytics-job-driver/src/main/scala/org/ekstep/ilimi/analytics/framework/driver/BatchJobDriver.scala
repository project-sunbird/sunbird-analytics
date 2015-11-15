package org.ekstep.ilimi.analytics.framework.driver

import org.ekstep.ilimi.analytics.framework.DataFetcher
import org.ekstep.ilimi.analytics.framework.DataFilter
import org.ekstep.ilimi.analytics.framework.JobConfig
import org.ekstep.ilimi.analytics.framework.JobRunner
import org.ekstep.ilimi.analytics.framework.OutputDispatcher
import org.ekstep.ilimi.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
object BatchJobDriver {

    def process(config: JobConfig) {

        val p = CommonUtil.getParallelization(config);
        val sc = CommonUtil.getSparkContext(p, config.appName.getOrElse(config.model));
        val rdd = DataFetcher.fetchBatchData(sc, config.search, p);
        val filterRdd = DataFilter.filterAndSort(rdd, config.filters, config.sort);
        val output = JobRunner.executeBatch(config.model, sc, filterRdd, config.modelParams);
        OutputDispatcher.dispatch(config.output, output);
    }
}