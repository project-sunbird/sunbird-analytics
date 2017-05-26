package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.DBUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.model.SparkSpec
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.DerivedEvent

class TestUpdateAuthorMetricsDB extends SparkSpec(null) {
  
    "UpdateAuthorMetricsDB" should "update author metrics DB" in {
        DBUtil.truncateTable(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT)
        
        val input = loadFile[DerivedEvent]("src/test/resources/author-usage-updater/test.log");
        UpdateAuthorMetricsDB.execute(input, Option(Map()))
        
        val data = sc.cassandraTable[AuthorMetricsFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT).map{x=> x}.collect
        data.length should be (2)
        data(0).d_period should be (20170520)
        data.last.d_period should be (20170522)
    }
}