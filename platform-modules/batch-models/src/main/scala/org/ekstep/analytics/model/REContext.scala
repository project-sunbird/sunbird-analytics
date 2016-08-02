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
case class LabeledData(data: LabeledPoint) extends AlgoOutput with Output

object REContext extends IBatchModelTemplate[Empty, DeviceDetails, LabeledData, LabeledData] with Serializable {

    val className = "org.ekstep.analytics.model.REContext"
    override def name(): String = "REContext"

    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceDetails] = {

        val device_usage = sc.cassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map { x => (x.device_id, x) }
        val allDevices = device_usage.map(x => DeviceId(x._1)).distinct;
        val device_spec = allDevices.joinWithCassandraTable[DeviceSpec](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_SPECIFICATION_TABLE).map { x => (x._1.device_id, x._2) }
        val device_info = device_usage.leftOuterJoin(device_spec)
        val device_content_usage = allDevices.joinWithCassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).groupBy(f => f._1.device_id).mapValues(f => f.map(x => x._2));
        val device_data = device_info.leftOuterJoin(device_content_usage)
        device_data.map { x =>
            DeviceDetails(x._1, x._2._1._1, x._2._1._2, x._2._2)
        };

    }

    override def algorithm(data: RDD[DeviceDetails], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LabeledData] = {

        val contents = sc.broadcast(data.map { x => x.device_content.get.map { y => y.content_id } }.distinct().flatMap { x => x })

        data.map { x =>
            contents.value.map { contentid =>
                val device_id = x.device_id
                println(device_id + " " + contentid)
                val available_contents_info = x.device_content.get.map { x => (x.content_id, x) }.toMap
                val device_content_details = available_contents_info.getOrElse(contentid, DeviceContentSummary(device_id, contentid, None, None, None, None, None, None, None, None, None, None, None, None))
                val target_variable = device_content_details.total_timespent.getOrElse(0.0)

            }
        }
        null
    }

    override def postProcess(data: RDD[LabeledData], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LabeledData] = {
        null
    }

}