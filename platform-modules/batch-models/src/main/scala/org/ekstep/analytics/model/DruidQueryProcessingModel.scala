package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._

case class DruidOutput(context_pdata_id: Option[String], context_pdata_pid: Option[String], total_duration: Option[Double],
                       count: Option[Int], dimensions_channel: Option[String], dimensions_sid: Option[String], dimensions_pdata_id: Option[String],
                       dimensions_type: Option[String], dimensions_mode: Option[String], dimensions_did: Option[String], object_id: Option[String],
                       content_board: Option[String], total_ts: Option[Double]) extends Input with AlgoInput with AlgoOutput with Output

object DruidQueryProcessingModel  extends IBatchModelTemplate[DruidOutput, DruidOutput, DruidOutput, DruidOutput] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DruidQueryProcessingModel"
    override def name: String = "DruidQueryProcessingModel"

    override def preProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {
        data
    }

    override def algorithm(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {
        data
    }

    override def postProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {
        data
    }
}
