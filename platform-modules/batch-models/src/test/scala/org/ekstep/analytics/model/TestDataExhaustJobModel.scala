package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.CommonUtil

class TestDataExhaustJobModel extends SparkSpec(null) {
  
    "DataExhaustJobModel" should "generate zip file of all events and save to s3" in {

        
    }
    
}