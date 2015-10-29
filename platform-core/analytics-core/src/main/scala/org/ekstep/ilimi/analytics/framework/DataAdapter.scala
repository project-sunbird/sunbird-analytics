package org.ekstep.ilimi.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * @author Santhosh
 */
object DataAdapter {
  
    def getUserData(sc: SparkContext): RDD[(String, User)] = {
        null;
    }
}