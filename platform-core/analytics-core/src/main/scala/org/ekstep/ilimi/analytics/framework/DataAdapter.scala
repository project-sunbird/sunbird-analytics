package org.ekstep.ilimi.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.adapter.UserAdapter


/**
 * @author Santhosh
 */
object DataAdapter {
  
    def getUserData(sc: SparkContext): RDD[(String, User)] = {
        sc.parallelize(UserAdapter.getUserMapping().toSeq, 10);
    }
}