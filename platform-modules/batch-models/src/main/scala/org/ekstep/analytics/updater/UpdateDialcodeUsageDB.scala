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
case class DialCodeIndex(dial_code : String, period : Int, channel : String)

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

      val dialcode_sessions = data.map{ x =>
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
    data.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE,
      SomeColumns("dial_code", "period", "channel", "total_dial_scans_local", "total_dial_scans_global", "first_scan", "last_scan",
        "average_scans_per_day", "updated_date"))
    data.map{ x =>
      val propertiesMap = Map(
        "total_dial_scans_local" -> x.total_dial_scans_local,
        "total_dial_scans_global" -> x.total_dial_scans_global,
        "first_scan" -> x.first_scan,
        "last_scan" -> x.last_scan,
        "average_scans_per_day" -> x.average_scans_per_day
      )
      val finalMap = propertiesMap.filter(x => x._1.nonEmpty).map{ x => x._1 -> Map("ov" -> null, "nv" -> x._2)}
      GraphUpdateEvent(DateTime.now().getMillis, x.dial_code, Map("properties" -> finalMap), "", nodeType = "DIALCODE_METRICS")
    }
  }

  private def rollup(data : RDD[DialCodeUsage_T], period: Period) : RDD[DialCodeUsage] = {

    val current_data = data.filter(x => !(x.dial_code.equals(""))).map { x =>
      val d_period = CommonUtil.getPeriod(x.last_gen_date.getMillis, period)
      val newDialCode = DialCodeUsage(x.dial_code, d_period, x.channel, x.total_dial_scans_local, x.total_dial_scans_global, x.first_scan, x.last_scan,
        x.average_scans_per_day, x.updated_date)
      (DialCodeIndex(x.dial_code, d_period, x.channel), newDialCode)
    }.reduceByKey(reduce)

    val previous_data = current_data.map{ x => x._1}.joinWithCassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).on(SomeColumns("dial_code", "period", "channel"))
    val joined_data = current_data.leftOuterJoin(previous_data)
    val rollup_summaries = joined_data. map{ x =>
      val index = x._1
      val new_matrix = x._2._1
      val old_matrix = x._2._2.getOrElse(DialCodeUsage(index.dial_code, index.period, new_matrix.channel, 0L, 0L, new_matrix.first_scan, 0L, 0L, System.currentTimeMillis()))
      reduce(old_matrix, new_matrix)
    }
    rollup_summaries
  }
  private def reduce(old_values : DialCodeUsage, new_value : DialCodeUsage) : DialCodeUsage = {
    val total_dial_scans = old_values.total_dial_scans_local + new_value.total_dial_scans_local
    val first_scan = if(old_values.first_scan < new_value.first_scan) old_values.first_scan else new_value.first_scan
    val last_scan = if(old_values.last_scan > new_value.last_scan) old_values.last_scan else new_value.last_scan
    val average_scan_per_day = getAverageScanPerDay(total_dial_scans, getDaysBetween(first_scan, last_scan))
    DialCodeUsage(new_value.dial_code, new_value.period, new_value.channel, total_dial_scans, total_dial_scans, first_scan,
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