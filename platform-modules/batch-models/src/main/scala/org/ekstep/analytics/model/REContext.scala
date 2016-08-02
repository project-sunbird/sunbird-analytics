package org.ekstep.analytics.model

import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework._

case class DeviceDetails(device_id: String, device_usage: DeviceUsageSummary, device_spec: Option[DeviceSpec], device_content: Option[Iterable[DeviceContentSummary]]) extends AlgoInput
case class ContentToVector(content_id: String, text_vec: List[Double], tag_vec: List[Double])
case class EmptyClass() extends Input
case class LabeledPointRDD() extends AlgoOutput with Output

object REContext extends IBatchModelTemplate[EmptyClass, DeviceDetails, LabeledPointRDD, LabeledPointRDD] with Serializable {

    val className = "org.ekstep.analytics.model.REContext"
    override def name(): String = "REContext"

    override def preProcess(data: RDD[EmptyClass], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceDetails] = {

        val device_usage = sc.cassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => (x.device_id, x) }
        val allDevices = device_usage.map(x => DeviceId(x._1)).distinct;
        val device_spec = allDevices.joinWithCassandraTable[DeviceSpec](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE).map { x => (x._1.device_id, x._2) }
        val device_info = device_usage.leftOuterJoin(device_spec)
        val device_content_usage = allDevices.joinWithCassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).groupBy(f => f._1.device_id).mapValues(f => f.map(x => x._2));
        val device_data = device_info.leftOuterJoin(device_content_usage)

        device_data.map { x =>
            DeviceDetails(x._1, x._2._1._1, x._2._1._2, x._2._2)
        }

    }

    override def algorithm(data: RDD[DeviceDetails], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LabeledPointRDD] = {

        val content_vec = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC).map { x => (x.content_id, x) }
        data.map { data =>
            //            val target_variable = data.device_content.get.device_content.total_timespent.getOrElse(0.0)
            //            var vec_arr : Array[Double] = new Array[Double](285)
            //            val text_vec = data.device_content.get.content_to_vec.get.text_vec.toArray
            //            val tag_vec = data.device_content.get.content_to_vec.get.tag_vec
            //            val content_attributes = data.device_content.get.device_content
            //            val device_attributes = data.device_usage.get
            //            val device_spec = data.device_spec
            //            LabeledPoint(target_variable, Vectors.dense(1.0, 0.0, 3.0))

            null
        }
    }

    override def postProcess(data: RDD[LabeledPointRDD], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LabeledPointRDD] = {
        null
    }

}