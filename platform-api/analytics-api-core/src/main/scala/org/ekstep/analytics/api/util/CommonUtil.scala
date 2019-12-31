package org.ekstep.analytics.api.util

import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.ekstep.analytics.api.{ExperimentBodyResponse, ExperimentParams, Params, Range, Response, ResponseCode}
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


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

        new SparkContext(conf);
        // $COVERAGE-ON$
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
        val startMonth = endDate.minusMonths(count);
        Range(monthPeriod.print(startMonth).toInt, monthPeriod.print(endDate).toInt)
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