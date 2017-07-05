package org.ekstep.analytics.metrics.job

import org.ekstep.analytics.framework.IBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.CassandraTable
import org.apache.commons.lang3.StringUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.util.ConfigDetails
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone

trait MetricsBatchModel[T, R] extends IBatchModel[T, R] {

    def processQueryAndComputeMetrics[T <: CassandraTable](details: ConfigDetails, groupFn: (T) => String)(implicit mf: Manifest[T], sc: SparkContext): RDD[(String, Array[T])] = {
        val filterFn = (x: String) => { StringUtils.split(x, "-").apply(0).size == 8 };
        val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
        val start_date = dateFormat.parseDateTime(details.periodfrom).getMillis
        val end_date = CommonUtil.getEndTimestampOfDay(details.periodUpTo)
        val rdd = sc.cassandraTable[T](details.keyspace, details.table).where("updated_date>=?", start_date).where("updated_date<=?", end_date)
        val files = rdd.groupBy { x => groupFn(x) }.filter { x => filterFn(x._1) }.mapValues { x => x.toArray }
        files;
    }

    def getMeasuredEvent[T <: CassandraTable](eid: String, mid: String, channel: String, modelName: String, metrics: Map[String, AnyRef], dimension: Dimensions): MeasuredEvent = {

        MeasuredEvent(eid, System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, "", Option(channel), None, None,
            Context(PData("AnalyticsDataPipeline", "1.0", Option(modelName)), None, "DAY", null),
            dimension,
            MEEdata(metrics));
    }

    def saveToS3(data: RDD[(String, Array[String])], details: ConfigDetails)(implicit sc: SparkContext): Long = {
        data.map { x =>
            val period = StringUtils.split(x._1, "-").apply(0)
            val periodDateTime = CommonUtil.dayPeriod.parseDateTime(period).withTimeAtStartOfDay()
            val periodString = StringUtils.split(periodDateTime.toString(), "T").apply(0)
            val ts = periodDateTime.getMillis
            val fileKey = details.filePrefix + periodString + "-" + ts + details.fileSuffix;
            OutputDispatcher.dispatch(Dispatcher(details.dispatchTo, details.dispatchParams ++ Map("key" -> fileKey, "file" -> fileKey)), x._2)
        }.count();
    }
}