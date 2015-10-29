package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
object DataFilter {

    /**
     * Execute multiple filters
     */
    def filterAndSort(events: RDD[Event], filters: Option[Array[Filter]], sort: Option[Sort]): RDD[Event] = {
        null;
    }
}