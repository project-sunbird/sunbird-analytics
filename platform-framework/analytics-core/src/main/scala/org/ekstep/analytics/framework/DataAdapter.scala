package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.adapter.UserAdapter


/**
 * @author Santhosh
 */
object DataAdapter {
  
    def getUserData(sc: SparkContext): RDD[(String, UserProfile)] = {
        sc.parallelize(UserAdapter.getUserProfileMapping().toSeq, 10);
    }
}