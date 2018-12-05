package org.ekstep.analytics.updater

import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.joda.time.{DateTime, DateTimeZone, LocalDate}

case class DialCodeUsage(dial_code: String, period: Int, channel: String, total_dial_scans_local: Long, total_dial_scans_global: Long, first_scan: Long, last_scan: Long, average_scans_per_day: Long, updated_date: Long) extends AlgoOutput with Output with CassandraTable
case class DialCodeUsage_T(dial_code: String, period: Int, channel: String, total_dial_scans_local: Long, total_dial_scans_global: Long, first_scan: Long, last_scan: Long, average_scans_per_day: Long, updated_date: Long, last_gen_date: DateTime)
case class DialCodePrimaryKey(dial_code : String, period : Int, channel : String)

object UpdateDialcodeUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, DialCodeUsage, GraphUpdateEvent] with Serializable {

  val className = "org.ekstep.analytics.updater.UpdateDialcodeUsageDB"
  override def name(): String = "UpdateDialcodeUsageDB"
  /**
    * Pre processing steps before running the algorithm. Few pre-process steps are
    * 1. Transforming input - Filter/Map etc.
    * 2. Join/fetch data from LP
    * 3. Join/Fetch data from Cassandra
    */
  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext) : RDD[DerivedEvent] = {

    DataFilter.filter(data, Filter("eid", "EQ", Option("ME_DIALCODE_USAGE_SUMMARY")))
  }

  /**
    * Method which runs the actual algorithm
    */
  override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext) : RDD[DialCodeUsage] = {

    val dialcode_sessions =
      data.map { x =>
        val eks_map = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val dial_code = x.dimensions.dial_code.getOrElse("")
        val period = x.dimensions.period.getOrElse(0)
        val channel = CommonUtil.getChannelId(x)
        val total_dial_scans_local = eks_map.getOrElse("total_dial_scans", 0L).asInstanceOf[Number].longValue()
        val total_dial_scans_global = total_dial_scans_local
        val first_scan = eks_map.getOrElse("first_scan", 0L).asInstanceOf[Number].longValue()
        val last_scan = eks_map.getOrElse("last_scan", 0L).asInstanceOf[Number].longValue()
        val average_scans_per_day = eks_map.getOrElse("total_dial_scans", 0L).asInstanceOf[Number].longValue()
        DialCodeUsage_T(dial_code, period, channel, total_dial_scans_local, total_dial_scans_global, first_scan,
          last_scan, average_scans_per_day, System.currentTimeMillis(), new DateTime(x.context.date_range.to))
      }
    rollup(dialcode_sessions, DAY).union(rollup(dialcode_sessions, WEEK)).union(rollup(dialcode_sessions, MONTH)).union(rollup(dialcode_sessions, CUMULATIVE)).cache()

  }

  /**
    * Post processing on the algorithm output. Some of the post processing steps are
    * 1. Saving data to Cassandra
    * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
    * 3. Transform into a structure that can be input to another data product
    */
  override def postProcess(data: RDD[DialCodeUsage], config: Map[String, AnyRef])(implicit sc: SparkContext) : RDD[GraphUpdateEvent] = {
    // Save the computed cumulative aggregates to Cassandra
    data.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE,
      SomeColumns("dial_code", "period", "channel", "total_dial_scans_local", "total_dial_scans_global", "first_scan", "last_scan",
        "average_scans_per_day", "updated_date"))
    // Generate a transaction event to index into Elasticsearch
    data.map{ x =>
      val propertiesMap = Map(
        "total_dial_scans_local" -> x.total_dial_scans_local,
        "total_dial_scans_global" -> x.total_dial_scans_global,
        "first_scan" -> x.first_scan,
        "last_scan" -> x.last_scan,
        "average_scans_per_day" -> x.average_scans_per_day
      )
      val finalMap = propertiesMap.filter(x => x._1.nonEmpty).map{ x => x._1 -> Map("ov" -> null, "nv" -> x._2)}
      GraphUpdateEvent(ets = DateTime.now().getMillis, nodeUniqueId = x.dial_code,
        transactionData = Map("properties" -> finalMap), objectType = "", nodeType = "DIALCODE_METRICS")
    }
  }

  private def rollup(data : RDD[DialCodeUsage_T], period: Period) : RDD[DialCodeUsage] = {

    val current_data = data.filter(_.dial_code.nonEmpty).map { x =>
      val d_period = CommonUtil.getPeriod(x.last_gen_date.getMillis, period)
      val dialCodeUsage = DialCodeUsage(x.dial_code, d_period, x.channel, x.total_dial_scans_local,
        x.total_dial_scans_global, x.first_scan, x.last_scan, x.average_scans_per_day, x.updated_date)
      (DialCodePrimaryKey(x.dial_code, d_period, x.channel), dialCodeUsage)
    }.reduceByKey(reduce)

    val previous_data = current_data.map{ x => x._1}
      .joinWithCassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE)
      .on(SomeColumns("dial_code", "period", "channel"))
    val joined_data = current_data.leftOuterJoin(previous_data)
    val rollup_summaries = joined_data.map{ x =>
      val key = x._1
      val dialCodeUsageFromTelemetry = x._2._1
      val cumulativeDialCodeUsageFromCassandra = x._2._2.getOrElse(DialCodeUsage(key.dial_code, key.period,
        dialCodeUsageFromTelemetry.channel, 0L, 0L, dialCodeUsageFromTelemetry.first_scan, 0L, 0L, System.currentTimeMillis()))

      reduce(cumulativeDialCodeUsageFromCassandra, dialCodeUsageFromTelemetry)
    }
    rollup_summaries
  }

  private def reduce(oldValues: DialCodeUsage, newValue: DialCodeUsage) : DialCodeUsage = {
    val total_dial_scans = oldValues.total_dial_scans_local + newValue.total_dial_scans_local
    val first_scan = if(oldValues.first_scan < newValue.first_scan) oldValues.first_scan else newValue.first_scan
    val last_scan = if(oldValues.last_scan > newValue.last_scan) oldValues.last_scan else newValue.last_scan
    val average_scan_per_day = getAverageScanPerDay(total_dial_scans, getDaysBetween(first_scan, last_scan))
    DialCodeUsage(newValue.dial_code, newValue.period, newValue.channel, total_dial_scans, total_dial_scans, first_scan,
      last_scan, average_scan_per_day, System.currentTimeMillis())
  }

  private def getDaysBetween(start_ts : Long, end_ts : Long) : Int = {
    val start_date = new LocalDate(start_ts, DateTimeZone.UTC)
    val end_date = new LocalDate(end_ts, DateTimeZone.UTC)
    CommonUtil.daysBetween(start_date, end_date)
  }

  private def getAverageScanPerDay(total_dial_scans : Long, days : Int): Long ={
    if(days == 0) total_dial_scans else total_dial_scans/days
  }
}