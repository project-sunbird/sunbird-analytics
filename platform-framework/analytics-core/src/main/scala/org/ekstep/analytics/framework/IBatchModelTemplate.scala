package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

trait IBatchModelTemplate[T, A, B, R] extends IBatchModel[T, R] {

    /**
     * 
     */
    override def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[R] = {
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        postProcess(algorithm(preProcess(events, config), config), config)
    }

    /**
     * 
     */
    def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[A]

    /**
     * 
     */
    def algorithm(events: RDD[A], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[B]

    /**
     * 
     */
    def postProcess(events: RDD[B], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[R]
}