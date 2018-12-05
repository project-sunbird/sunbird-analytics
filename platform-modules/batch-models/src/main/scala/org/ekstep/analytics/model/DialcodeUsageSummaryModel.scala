package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil

import scala.collection.mutable.ListBuffer

case class DialcodeScanMetrics(dialCode: String, channel: String, firstScan: Long, lastScan: Long, count: Int) extends AlgoOutput

object DialcodeUsageSummaryModel extends IBatchModelTemplate[V3Event, V3Event, DialcodeScanMetrics, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DialcodeUsageSummaryModel"

    override def name: String = "DialcodeUsageSummaryModel"

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3Event] = {
        data
    }

    override def algorithm(input: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DialcodeScanMetrics] = {
        def mapDialCodes(events: RDD[V3Event]): RDD[(List[String], Long, String)] = events.map(event => {
            var dialcode = new ListBuffer[String]()
            val filters = event.edata.filters.getOrElse(Map()).asInstanceOf[Map[String, AnyRef]]

            if (filters("dialcodes").isInstanceOf[String]) {
                dialcode += filters.getOrElse("dialcodes", "").asInstanceOf[String]
            } else {
                dialcode ++= filters.getOrElse("dialcodes", List()).asInstanceOf[List[String]]
            }
            (dialcode.toList, event.ets, event.context.channel)
        })

        def filterByDialcodeSearch(events: RDD[V3Event]): RDD[V3Event] = events.filter(_.edata.filters.getOrElse(Map()).asInstanceOf[Map[String, AnyRef]].contains("dialcodes"))
        def deriveTimestamp(data: Iterable[((String, String), (Long, Int))]): (Long, Long) = {
            val scanTimestamp = data.map(x => x._2._1).toList.sorted
            (scanTimestamp.head, scanTimestamp.last)
        }
        def totalScanCount(data: Iterable[((String, String), (Long, Int))]): Int = data.map(x => x._2._2).sum

        val filteredData: RDD[V3Event] = filterByDialcodeSearch(input)
        mapDialCodes(filteredData)
            .map(x => x._1.map(t => ((t, x._3), (x._2, 1)))) // ((dialcode, channel), (ets, count))
            .flatMap(identity)
            .groupBy(_._1) // group by (dialcode, channel)
            .map(x => (x._1, deriveTimestamp(x._2), totalScanCount(x._2))) // ((dialcode, channel), (firstScan, lastScan), count)
            .map(x => DialcodeScanMetrics(x._1._1, x._1._2, x._2._1, x._2._2, x._3))
    }

    override def postProcess(data: RDD[DialcodeScanMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val ME_ID = "ME_DIALCODE_USAGE_SUMMARY"
        val meEventVersion = AppConf.getConfig("telemetry.version")

        val output = data.map { metric =>
            val mid = CommonUtil.getMessageId(ME_ID, metric.dialCode, "DAY", metric.firstScan, None, Some(metric.channel))
            val measures = Map("total_dial_scans" -> metric.count, "first_scan" -> metric.firstScan, "last_scan" -> metric.lastScan)

            MeasuredEvent(ME_ID, System.currentTimeMillis(), System.currentTimeMillis(), meEventVersion, mid, "", "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "DialcodeUsageSummarizer").asInstanceOf[String])), None, "DAY", DtRange(0, 0)),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(metric.channel), None, None, None, Some(metric.dialCode)),
                MEEdata(measures), None);
        }
        output
    }
}