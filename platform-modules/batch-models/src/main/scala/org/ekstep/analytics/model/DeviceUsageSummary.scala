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

case class UsageSummary(start_time: Long, end_time: Long, num_days: Long, avg_num_launches: Double, avg_time: Double)

object DeviceUsageSummary extends IBatchModel[MeasuredEvent] with Serializable {
  
      def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = 
      {
        println("### Running the model DeviceUsageSummary ###");
        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_GENIE_SUMMARY")));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
        
        val groupedEvents = filteredEvents.map(event => (event.dimensions.did.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);
        
        val deviceUsageSummary = groupedEvents.mapValues { events =>
          
          val eventsSortedByTS = events.sortBy { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_stamp").get.asInstanceOf[Long] };
          val eventsSortedByDateRange = events.sortBy { x => x.context.date_range.from.asInstanceOf[Long] };
          val start_time = eventsSortedByDateRange.head.context.date_range.from.asInstanceOf[Long]
          val end_time = eventsSortedByTS.last.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_stamp").get.asInstanceOf[Long]
          val num_days:Long = CommonUtil.datesBetween(new LocalDate(start_time),new LocalDate(end_time)).size
          val avg_num_launches = (events.size/num_days).asInstanceOf[Double]
          val totalTimeSpent = events.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
            }.sum
          val avg_time = totalTimeSpent/num_days 
          
          (UsageSummary(start_time,end_time,num_days,avg_num_launches,avg_time))
        }.cache();
        
        null
      }
      
      private def getMeasuredEvent(usageSummary: UsageSummary, deviceID:String,  config: Map[String, AnyRef]): MeasuredEvent = {

        val mid = deviceID;
        val measures = Map(
            "start_time" -> usageSummary.start_time,
            "end_time" -> usageSummary.end_time,
            "num_days" -> usageSummary.num_days,
            "avg_num_launches" -> usageSummary.avg_num_launches,
            "avg_time" -> usageSummary.avg_time);
        MeasuredEvent("ME_DEVICE_USAGE_SUMMARY", System.currentTimeMillis(), usageSummary.end_time, "1.0", mid, None, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "DeviceUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], DtRange(usageSummary.start_time,usageSummary.end_time)),
            Dimensions(None, Option(deviceID) , None, None, None, None, None),
            MEEdata(measures));
    }
}