/**
 * @author Santhosh Vasabhaktula
 */
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.connector.InfluxDB._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.AlgoOutput
import org.apache.spark.util.Distribution
import org.ekstep.analytics.updater.IInfluxDBUpdater
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.util.BEEvent



case class APIUsageSummary(rid: String, path: String, method: String, count: Int, min: Double, max: Double, average: Double) extends AlgoOutput with Output

/**
 * @dataproduct
 * @Summarizer
 *
 * APIUsageSummaryModel
 *
 * Functionality
 * 1. Generate app specific session summary events. This would be used to compute app usage metrics.
 * Events used - BE_OBJECT_LIFECYCLE, CP_SESSION_START, CE_START, CE_END, CP_INTERACT & CP_IMPRESSION
 */
object APIUsageSummaryModel extends IBatchModelTemplate[BEEvent, BEEvent, APIUsageSummary, Empty] with IInfluxDBUpdater with Serializable {

    implicit val className = "org.ekstep.analytics.model.AppSessionSummaryModel"
    override def name: String = "AppSessionSummaryModel"

    override def preProcess(data: RDD[BEEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[BEEvent] = {
        data.filter { x => x.eid.equals("BE_ACCESS") };
    }

    override def algorithm(data: RDD[BEEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[APIUsageSummary] = {
        data.groupBy { x => x.edata.eks.rid.getOrElse("UNKNOWN") }.map(f => {
            val durations = f._2.map { x => x.edata.eks.duration.getOrElse(0) }.map { x => x.toDouble };
            val firstRecord = f._2.head.edata.eks;
            APIUsageSummary(f._1, firstRecord.path.getOrElse("UNKNOWN"), firstRecord.method.getOrElse("UNKNOWN"), f._2.size, durations.min, durations.max, CommonUtil.roundDouble(durations.sum.toDouble / durations.size, 2))
        })
    }

    override def postProcess(data: RDD[APIUsageSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        val metrics = data.map { x =>  
            InfluxRecord(Map(), Map(
                "rid" -> x.rid,
                "path" -> x.path,
                "method" -> x.method,
                "count" -> x.count.asInstanceOf[AnyRef],
                "min" -> x.min.asInstanceOf[AnyRef],
                "max" -> x.max.asInstanceOf[AnyRef],
                "average" -> x.average.asInstanceOf[AnyRef]
            ));
        }
        metrics.saveToInflux("api_usage_stats"); // Update code to save it infra influx DB
        sc.makeRDD(Seq(Empty()));
    }
}