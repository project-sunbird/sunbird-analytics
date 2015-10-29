package org.ekstep.ilimi.analytics.framework.driver

import org.ekstep.ilimi.analytics.framework.JobConfig
import org.ekstep.ilimi.analytics.framework.DataFetcher
import org.ekstep.ilimi.analytics.framework.DataFilter
import org.ekstep.ilimi.analytics.framework.JobRunner
import org.ekstep.ilimi.analytics.framework.OutputDispatcher


/**
 * @author Santhosh
 */
object BatchJobDriver {

    def process(config: JobConfig) {

        val rdd = DataFetcher.getBatchData(config.search);
        val filterRdd = DataFilter.filterAndSort(rdd, config.filters, config.sort);
        val output = JobRunner.executeBatch(config.model, filterRdd, config.modelParams);
        OutputDispatcher.dispatch(config.output, output);
    }
}