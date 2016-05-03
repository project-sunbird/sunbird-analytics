package org.ekstep.analytics.updater

import org.scalatest._
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
class TestUpdateContentPopularity extends SparkSpec(null) {
  
    "UpdateContentPopularity" should "populate total sessions count as popularity on content" in {
        
        val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data.json"))))));
        val rdd2 = UpdateContentPopularity.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be (8);
    }
    
}