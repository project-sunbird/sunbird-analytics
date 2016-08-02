package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD

class TestREContext extends SparkSpec(null) {
  
    "REContext" should "generate libsvm files" in {
        val ec = List(new EmptyClass())
        val rdd = ec.map(x => x)
        //val me = REContext.execute(rdd, None);
    }
}