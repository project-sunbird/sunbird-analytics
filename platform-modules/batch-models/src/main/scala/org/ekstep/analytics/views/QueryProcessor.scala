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
        val periods = getPeriods(view.periodType, view.periodUpTo);
        val rdd = sc.cassandraTable[T](view.keyspace, view.table).where("d_period IN ?", periods.toList);
        val files = rdd.groupBy { x => groupFn(x) }.mapValues { x => x.map { y => JSONUtils.serialize(y) } }.mapValues { x => x.toArray }
        files;
    }

    @throws(classOf[Exception])
    def getPeriods(periodType: String, periodUpTo: Int): Array[Int] = {
        Period.withName(periodType) match {
            case DAY   => getDayPeriods(periodUpTo);
            case MONTH => getMonthPeriods(periodUpTo);
            case WEEK  => getWeekPeriods(periodUpTo);
            case CUMULATIVE  => Array(0);
        }
    }

    private def getDayPeriods(count: Int): Array[Int] = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val x = for (i <- 1 to (count + 1)) yield CommonUtil.getPeriod(endDate.minusDays(i), DAY);
        x.toArray;
    }

    private def getMonthPeriods(count: Int): Array[Int] = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val x = for (i <- 0 to count) yield CommonUtil.getPeriod(endDate.minusMonths(i), MONTH);
        x.toArray;
    }

    private def getWeekPeriods(count: Int): Array[Int] = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val x = for (i <- 0 to count) yield CommonUtil.getPeriod(endDate.minusWeeks(i), WEEK);
        x.toArray;
    }

}