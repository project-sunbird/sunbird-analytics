package org.ekstep.analytics.views

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.CassandraTable
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Period
import org.ekstep.analytics.framework.Period._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.rdd.RDD

object QueryProcessor {

    def processQuery[T <: CassandraTable](view: View, groupFn: (T) => String)(implicit mf: Manifest[T], sc: SparkContext): RDD[(String, Array[String])] = {
        val periods = CommonUtil.getPeriods(view.periodType, view.periodUpTo);
        val rdd = sc.cassandraTable[T](view.keyspace, view.table).where("d_period IN ?", periods.toList);
        val files = rdd.groupBy { x => groupFn(x) }.mapValues { x => x.map { y => JSONUtils.serialize(y) } }.mapValues { x => x.toArray }
        files;
    }
}