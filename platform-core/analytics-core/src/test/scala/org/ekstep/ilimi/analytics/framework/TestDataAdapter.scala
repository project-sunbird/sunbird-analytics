package org.ekstep.ilimi.analytics.framework

/**
 * @author Santhosh
 */
class TestDataAdapter extends SparkSpec {
    
    "DataAdapter" should "return an RDD of users" in {
        
        val rdd = DataAdapter.getUserData(sc);
        rdd.count() should be > 0l
    }
  
}