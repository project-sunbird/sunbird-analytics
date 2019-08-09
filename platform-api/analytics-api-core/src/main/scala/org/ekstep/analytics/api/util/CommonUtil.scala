package org.ekstep.analytics.api.util

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.{ExperimentBodyResponse, ExperimentParams, Params, Range, Response, ResponseCode}
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Days
import org.joda.time.Duration
import org.joda.time.LocalDate
import org.joda.time.Weeks
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.commons.lang3.StringUtils
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory


/**
 * @author Santhosh
 */
object CommonUtil {

    @transient val dayPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    @transient val weekPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyy'7'ww").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    @transient val monthPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    @transient val weekPeriodLabel: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-ww").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    @transient val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withZoneUTC();
    @transient val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    
    def getSparkContext(parallelization: Int, appName: String): SparkContext = {
        val conf = new SparkConf().setAppName(appName);
        val master = conf.getOption("spark.master");
        // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered as they depend on environment variables
        if (master.isEmpty) {
            conf.setMaster("local[*]");
        }
        if (!conf.contains("spark.cassandra.connection.host")) {
            conf.set("spark.cassandra.connection.host", AppConf.getConfig("spark.cassandra.connection.host"))
        }
        if(embeddedCassandraMode)
          conf.set("spark.cassandra.connection.port", AppConf.getConfig("cassandra.service.embedded.connection.port"))
   
        // $COVERAGE-ON$
        new SparkContext(conf);
    }
    
    private def embeddedCassandraMode() : Boolean = {
    	val isEmbedded = AppConf.getConfig("cassandra.service.embedded.enable");
    	StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded);
    }

    def closeSparkContext()(implicit sc: SparkContext) {
        sc.stop();
    }

    def roundDouble(value: Double, precision: Int): Double = {
        BigDecimal(value).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble;
    }

    def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
        val from = new LocalDate(fromDate, DateTimeZone.UTC)
        val to = new LocalDate(toDate, DateTimeZone.UTC)
        Weeks.weeksBetween(from, to).getWeeks;
    }

    def getDayRange(count: Int): Range = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val startDate = endDate.minusDays(count);
        Range(dayPeriod.print(startDate).toInt, dayPeriod.print(endDate).toInt)
    }

    def getMonthRange(count: Int): Range = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val startDate = endDate.minusDays(count * 30);
        Range(monthPeriod.print(startDate).toInt, monthPeriod.print(endDate).toInt)
    }

    def getWeekRange(count: Int): Range = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val startDate = endDate.minusDays(count * 7);
        Range(getWeekNumber(startDate.getWeekyear, startDate.getWeekOfWeekyear), getWeekNumber(endDate.getWeekyear, endDate.getWeekOfWeekyear))
    }

    private def getWeekNumber(year: Int, weekOfWeekyear: Int): Int = {

        if (weekOfWeekyear < 10) {
            (year + "70" + weekOfWeekyear).toInt
        } else {
            (year + "7" + weekOfWeekyear).toInt
        }
    }

    def errorResponse(apiId: String, err: String, responseCode: String): Response = {
        Response(apiId, "1.0", df.print(System.currentTimeMillis()),
            Params(UUID.randomUUID().toString, null, responseCode, "failed", err),
            responseCode, None)
    }

    def experimentErrorResponse(apiId: String, errResponse: Map[String, String], responseCode: String): ExperimentBodyResponse = {
        ExperimentBodyResponse(apiId, "1.0", df.print(System.currentTimeMillis()),
            ExperimentParams(UUID.randomUUID().toString, null, responseCode, "failed", errResponse),
            responseCode, None)
    }

    def experimentOkResponse(apiId: String, result: Map[String, AnyRef]): ExperimentBodyResponse = {
        ExperimentBodyResponse(apiId, "1.0", df.print(DateTime.now(DateTimeZone.UTC).getMillis), ExperimentParams(UUID.randomUUID().toString(), null, null, "successful", null), ResponseCode.OK.toString(), Option(result));
    }

    def errorResponseSerialized(apiId: String, err: String, responseCode: String): String = {
        JSONUtils.serialize(errorResponse(apiId, err, responseCode))
    }

    def OK(apiId: String, result: Map[String, AnyRef]): Response = {
        Response(apiId, "1.0", df.print(DateTime.now(DateTimeZone.UTC).getMillis), Params(UUID.randomUUID().toString(), null, null, "successful", null), ResponseCode.OK.toString(), Option(result));
    }

    def getRemainingHours(): Long = {
        val now = DateTime.now(DateTimeZone.UTC);
        new Duration(now, now.plusDays(1).withTimeAtStartOfDay()).getStandardHours;
    }

    def getPeriodLabel(period: Period, date: Int)(implicit config: Config): String = {
        val formatter = DateTimeFormat.forPattern("YYYYMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
        period match {
            case MONTH =>
                val format = config.getString("metrics.period.format.month");
                formatter.parseDateTime(date + "01").toString(DateTimeFormat.forPattern(format))
            case WEEK =>
                val datestr = Integer.toString(date);
                getWeekPeriodLabel(datestr);
            case DAY =>
                val format = config.getString("metrics.period.format.day")
                formatter.parseDateTime(date.toString()).toString(DateTimeFormat.forPattern(format))
            case _ => date.toString();
        }
    }
    
    def getWeekPeriodLabel(date: String): String = {
    	val week = date.substring(0,4)+"-"+date.substring(5,date.length);
    	val firstDay = weekPeriodLabel.parseDateTime(week);
        val lastDay = firstDay.plusDays(6);
        val first = firstDay.toString(DateTimeFormat.forPattern("MMM dd"));
        val last = if (firstDay.getMonthOfYear == lastDay.getMonthOfYear) 
        	lastDay.toString(DateTimeFormat.forPattern("dd"))
        else
        	lastDay.toString(DateTimeFormat.forPattern("MMM dd"))
        s"$first - $last";
    }
    
    def getMillis(): Long = {
    	DateTime.now(DateTimeZone.UTC).getMillis
    }
    
    def getToday(): String = {
        dateFormat.print(new DateTime)
    }
    
    def getPeriod(date: String): Int = {
        try {
            Integer.parseInt(date.replace("-", ""))  
        } catch {
          case t: Throwable => 0; // TODO: handle error
        }
    }
    
    def getDaysBetween(start: String, end: String): Int = {
        val to = dateFormat.parseLocalDate(end)
        val from = dateFormat.parseLocalDate(start)
        Days.daysBetween(from, to).getDays()
    }
    
    def caseClassToMap(ccObj: AnyRef) =
        (Map[String, AnyRef]() /: ccObj.getClass.getDeclaredFields) {
            (map, field) =>
                field.setAccessible(true)
                map + (field.getName -> field.get(ccObj))
        }

}