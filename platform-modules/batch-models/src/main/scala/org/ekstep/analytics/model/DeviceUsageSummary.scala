package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.LocalDate
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._

case class UsageSummary(device_id: String, start_time: Long, end_time: Long, num_days: Long, total_launches: Long, total_timespent: Double, avg_num_launches: Double, avg_time: Double)

case class DeviceId(device_id: String)

object DeviceUsageSummary extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
  
        println("### Running the model DeviceUsageSummary ###");
        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_GENIE_LAUNCH_SUMMARY")));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
  
        val newGroupedEvents = filteredEvents.map(event => (event.dimensions.did.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);
        val prevDeviceSummary = newGroupedEvents.map(f => DeviceId(f._1)).joinWithCassandraTable[UsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map(f => (f._1.device_id, f._2))
        val deviceData = newGroupedEvents.leftOuterJoin(prevDeviceSummary);

        val deviceUsageSummary = deviceData.mapValues { events =>
  
            val eventsSortedByTS = events._1.sortBy { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_stamp").get.asInstanceOf[Long] };
            val eventsSortedByDateRange = events._1.sortBy { x => x.context.date_range.from.asInstanceOf[Long] };
            val prevUsageSummary = events._2.getOrElse(UsageSummary(events._1.head.dimensions.did.get, 0L, 0L, 0L, 0L, 0.0, 0.0, 0.0));
            val eventStartTime = eventsSortedByDateRange.head.context.date_range.from.asInstanceOf[Long]
            val start_time = if (prevUsageSummary.start_time == 0) eventStartTime else if (eventStartTime > prevUsageSummary.start_time) prevUsageSummary.start_time else eventStartTime;
            val end_time = eventsSortedByTS.last.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_stamp").get.asInstanceOf[Long]
            val num_days: Long = CommonUtil.daysBetween(new LocalDate(start_time), new LocalDate(end_time))
            val num_launches = events._1.size + prevUsageSummary.total_launches
            val avg_num_launches = if (num_days == 0) num_launches else CommonUtil.roundDouble((num_launches / (num_days.asInstanceOf[Double])), 2)
            val totalTimeSpent = events._1.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
            }.sum + prevUsageSummary.total_timespent
            val avg_time = if (num_days == 0) totalTimeSpent else CommonUtil.roundDouble(totalTimeSpent / num_days, 2)
  
            (UsageSummary(events._1.head.dimensions.did.get, start_time, end_time, num_days, num_launches, totalTimeSpent, avg_num_launches, avg_time))
        }.cache();
  
        deviceUsageSummary.map(f => f._2).saveToCassandra(Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE);
  
        deviceUsageSummary.map(f => {
            getMeasuredEvent(f._2, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
  
    }

    private def getMeasuredEvent(usageSummary: UsageSummary, config: Map[String, AnyRef]): MeasuredEvent = {
  
        val mid = CommonUtil.getMessageId("ME_DEVICE_USAGE_SUMMARY", usageSummary.device_id, null, DtRange(0l,0l));
        val measures = Map(
            "start_time" -> usageSummary.start_time,
            "end_time" -> usageSummary.end_time,
            "num_days" -> usageSummary.num_days,
            "avg_num_launches" -> usageSummary.avg_num_launches,
            "avg_time" -> usageSummary.avg_time);
        MeasuredEvent("ME_DEVICE_USAGE_SUMMARY", System.currentTimeMillis(), usageSummary.end_time, "1.0", mid, "", None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "DeviceUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], DtRange(usageSummary.start_time, usageSummary.end_time)),
            Dimensions(None, Option(usageSummary.device_id), None, None, None, None, None),
            MEEdata(measures));
    }
}