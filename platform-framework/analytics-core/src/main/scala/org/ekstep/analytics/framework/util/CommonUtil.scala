package org.ekstep.analytics.framework.util

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.Paths.get
import java.nio.file.StandardCopyOption
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.GZIPOutputStream
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.joda.time.Days
import org.joda.time.LocalDate
import org.joda.time.Years
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jvalue2extractable
import org.json4s.string2JsonInput
import scala.collection.mutable.Buffer
import org.joda.time.Hours
import org.joda.time.DateTimeZone
import java.security.MessageDigest
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.Period._
import org.joda.time.Weeks

object CommonUtil {

    val className = "org.ekstep.analytics.framework.util.CommonUtil"
    @transient val df = new SimpleDateFormat("ssmmhhddMMyyyy");
    @transient val df2 = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssXXX");
    @transient val df3: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ").withZoneUTC();
    @transient val df5: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZoneUTC();
    @transient val df6: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC();
    @transient val df4: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    @transient val dayPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd");
    @transient val monthPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM");

    def getParallelization(config: JobConfig): Int = {

        val defParallelization = AppConf.getConfig("default.parallelization").toInt;
        config.parallelization.getOrElse(defParallelization);
    }

    def getSparkContext(parallelization: Int, appName: String): SparkContext = {

        JobLogger.debug("Initializing Spark Context", className)
        val conf = new SparkConf().setAppName(appName);
        val master = conf.getOption("spark.master");
        // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered as they depend on environment variables
        if (master.isEmpty) {
            JobLogger.info("Master not found. Setting it to local[*]", className)
            conf.setMaster("local[*]");
        }
        if (!conf.contains("spark.cassandra.connection.host")) {
            conf.set("spark.cassandra.connection.host", AppConf.getConfig("spark.cassandra.connection.host"))
        }
        // $COVERAGE-ON$
        val sc = new SparkContext(conf);
        setS3Conf(sc);
        JobLogger.debug("Spark Context initialized", className);
        sc;
    }

    def setS3Conf(sc: SparkContext) = {
        JobLogger.debug("CommonUtil: setS3Conf. Configuring S3 AccessKey& SecrateKey to SparkContext", className)
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getAwsKey());
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getAwsSecret());
    }

    def closeSparkContext()(implicit sc: SparkContext) {
        JobLogger.info("Closing Spark Context", className)
        sc.stop();
    }

    class Visitor extends java.nio.file.SimpleFileVisitor[java.nio.file.Path] {
        override def visitFile(
            file: java.nio.file.Path,
            attrs: java.nio.file.attribute.BasicFileAttributes): java.nio.file.FileVisitResult =
            {
                Files.delete(file);

                java.nio.file.FileVisitResult.CONTINUE;
            } // visitFile

        override def postVisitDirectory(
            dir: java.nio.file.Path,
            exc: IOException): java.nio.file.FileVisitResult =
            {
                Files.delete(dir);
                java.nio.file.FileVisitResult.CONTINUE;
            } // visitFile
    }

    def deleteDirectory(dir: String) {
        val path = get(dir);
        JobLogger.debug("Deleting directory " + path, className)
        Files.walkFileTree(path, new Visitor());
    }

    def deleteFile(file: String) {
        JobLogger.debug("Deleting file " + file, className)
        Files.delete(get(file));
    }

    def datesBetween(from: LocalDate, to: LocalDate): IndexedSeq[LocalDate] = {
        val numberOfDays = Days.daysBetween(from, to).getDays()
        for (f <- 0 to numberOfDays) yield from.plusDays(f)
    }

    def getStartDate(endDate: Option[String], delta: Int): Option[String] = {
        val to = if (endDate.nonEmpty) df4.parseLocalDate(endDate.get) else LocalDate.fromDateFields(new Date);
        Option(to.minusDays(delta).toString());
    }

    def getDatesBetween(fromDate: String, toDate: Option[String]): Array[String] = {
        val to = if (toDate.nonEmpty) df4.parseLocalDate(toDate.get) else LocalDate.fromDateFields(new Date);
        val from = df4.parseLocalDate(fromDate);
        val dates = datesBetween(from, to);
        dates.map { x => df4.print(x) }.toArray;
    }

    def daysBetween(from: LocalDate, to: LocalDate): Int = {
        Days.daysBetween(from, to).getDays();
    }

    def getEventTS(event: Event): Long = {
        if (event.ets > 0)
            event.ets
        else
            getTimestamp(event.ts);
    }

    def getEventSyncTS(event: Event): Long = {
        val timeInString = event.`@timestamp`;
        var ts = getTimestamp(timeInString, df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        if (ts == 0) {
            ts = getTimestamp(timeInString, df3, "yyyy-MM-dd'T'HH:mm:ssZZ");
        }
        if (ts == 0) {
            try {
                ts = getTimestamp(timeInString.substring(0, 19), df6, "yyyy-MM-dd'T'HH:mm:ss");
            } catch {
                case ex: Exception =>
                    ts = 0L;
            }
        }
        ts;
    }

    def getEventDate(event: Event): Date = {
        try {
            df3.parseLocalDate(event.ts).toDate;
        } catch {
            case _: Exception =>
                JobLogger.debug("Invalid event time - " + event.ts, className);
                null;
        }
    }

    def getGameId(event: Event): String = {
        if (event.gdata != null) event.gdata.id else null;
    }

    def getGameVersion(event: Event): String = {
        if (event.gdata != null) event.gdata.ver else null;
    }

    def getParallelization(config: Option[Map[String, String]]): Int = {
        getParallelization(config.getOrElse(Map[String, String]()));
    }

    def getParallelization(config: Map[String, String]): Int = {
        var parallelization = AppConf.getConfig("default.parallelization");
        if (config != null && config.nonEmpty) {
            parallelization = config.getOrElse("parallelization", parallelization);
        }
        parallelization.toInt;
    }

    def gzip(path: String): String = {
        val buf = new Array[Byte](1024);
        val src = new File(path);
        val dst = new File(path ++ ".gz");

        try {
            val in = new BufferedInputStream(new FileInputStream(src))
            try {
                val out = new GZIPOutputStream(new FileOutputStream(dst))
                try {
                    var n = in.read(buf)
                    while (n >= 0) {
                        out.write(buf, 0, n)
                        n = in.read(buf)
                    }
                } finally {
                    out.flush
                }
            } finally {
                in.close();
            }
        } catch {
            case e: Exception =>
                JobLogger.error("Error in gzip", className, e)
                throw e
        }
        path ++ ".gz";
    }

    def getAge(dob: Date): Int = {
        val birthdate = LocalDate.fromDateFields(dob);
        val now = new LocalDate();
        val age = Years.yearsBetween(birthdate, now);
        age.getYears;
    }

    def getTimeSpent(len: AnyRef): Option[Double] = {
        if (null != len) {
            if (len.isInstanceOf[String]) {
                Option(len.asInstanceOf[String].toDouble)
            } else if (len.isInstanceOf[Double]) {
                Option(len.asInstanceOf[Double])
            } else if (len.isInstanceOf[Int]) {
                Option(len.asInstanceOf[Int].toDouble)
            } else {
                Option(0d);
            }
        } else {
            Option(0d);
        }
    }

    def getTimeDiff(start: Event, end: Event): Option[Double] = {

        val st = getTimestamp(start.ts);
        val et = getTimestamp(end.ts);
        if (et == 0 || st == 0) {
            Option(0d);
        } else {
            Option(roundDouble(((et - st).toDouble / 1000), 2));
        }
    }

    def getTimeDiff(start: Long, end: Long): Option[Double] = {

        Option((end - start).toDouble / 1000);
    }

    def getHourOfDay(start: Long, end: Long): ListBuffer[Int] = {
        val hrList = ListBuffer[Int]();
        val startHr = new DateTime(start, DateTimeZone.UTC).getHourOfDay;
        val endHr = new DateTime(end, DateTimeZone.UTC).getHourOfDay;
        var hr = startHr;
        while (hr != endHr) {
            hrList += hr;
            hr = hr + 1;
            if (hr == 24) hr = 0;
        }
        hrList += endHr;
    }

    def getTimestamp(ts: String, df: DateTimeFormatter, pattern: String): Long = {
        try {
            df.parseDateTime(ts).getMillis;
        } catch {
            case _: Exception =>
                JobLogger.debug("Invalid time format - " + pattern + ts, className);
                0;
        }
    }

    def getTimestamp(timeInString: String): Long = {
        var ts = getTimestamp(timeInString, df3, "yyyy-MM-dd'T'HH:mm:ssZZ");
        if (ts == 0) {
            ts = getTimestamp(timeInString, df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        }
        if (ts == 0) {
            try {
                ts = getTimestamp(timeInString.substring(0, 19), df6, "yyyy-MM-dd'T'HH:mm:ss");
            } catch {
                case ex: Exception =>
                    ts = 0L;
            }
        }
        ts;
    }

    def getTags(metadata: Map[String, AnyRef]): Option[Array[String]] = {
        Option(metadata.getOrElse("tags", List[String]()).asInstanceOf[List[String]].toArray);
    }

    def roundDouble(value: Double, precision: Int): Double = {
        BigDecimal(value).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble;
    }

    def getMessageId(eventId: String, userId: String, granularity: String, dateRange: DtRange, contentId: String = "NA"): String = {
        val key = Array(eventId, userId, dateRange.from, dateRange.to, granularity, contentId).mkString("|");
        MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
    }

    def getMessageId(eventId: String, userId: String, granularity: String, syncDate: Long): String = {
        val key = Array(eventId, userId, df4.print(syncDate), granularity).mkString("|");
        MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
    }

    def getPeriod(syncts: Long, period: Period): Int = {
        val d = new DateTime(syncts, DateTimeZone.UTC);
        period match {
            case DAY        => dayPeriod.print(d).toInt;
            case WEEK       => (d.getWeekyear + "77" + d.getWeekOfWeekyear).toInt
            case MONTH      => monthPeriod.print(d).toInt
            case CUMULATIVE => 0
            case LAST7      => 7
            case LAST30     => 30
            case LAST90     => 90
            case _          => -1
        }
    }

    def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
        val from = new LocalDate(fromDate, DateTimeZone.UTC)
        val to = new LocalDate(toDate, DateTimeZone.UTC)
        Weeks.weeksBetween(from, to).getWeeks;
    }
}