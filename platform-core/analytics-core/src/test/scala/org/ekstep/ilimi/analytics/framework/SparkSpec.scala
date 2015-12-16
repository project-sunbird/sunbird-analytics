package org.ekstep.ilimi.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jvalue2extractable
import org.json4s.string2JsonInput
import org.scalatest.BeforeAndAfterAll
import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.ilimi.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
class SparkSpec extends BaseSpec with BeforeAndAfterAll {
    
    var events: RDD[Event] = null;
    var sc: SparkContext = null;
    
    override def beforeAll() {
        sc = CommonUtil.getSparkContext(1, "TestAnalyticsCore");
        val rdd = sc.textFile("src/test/resources/sample_telemetry.log", 1).cache();
        events = rdd.map { line =>
            {
                JSONUtils.deserialize[Event](line);
            }
        }.filter { x => x != null }.cache();
    }
    
    override def afterAll() {
        CommonUtil.closeSparkContext(sc);
    }
  
}