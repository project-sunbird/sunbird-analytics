package org.ekstep.analytics.framework.util

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.Paths.get
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.Date
import java.util.zip.GZIPOutputStream

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Days
import org.joda.time.LocalDate
import org.joda.time.Weeks
import org.joda.time.Years
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.json4s.jvalue2extractable
import org.json4s.string2JsonInput
import org.apache.commons.lang3.StringUtils

object CommonUtil {

    implicit val className = "org.ekstep.analytics.framework.util.CommonUtil"
    @transient val df3: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ").withZoneUTC();
    @transient val df5: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZoneUTC();
    @transient val df6: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC();
    @transient val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();
    @transient val weekPeriodLabel: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-ww").withZoneUTC();
    @transient val dayPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    @transient val monthPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));

    def getParallelization(config: JobConfig): Int = {

        val defParallelization = AppConf.getConfig("default.parallelization").toInt;
        config.parallelization.getOrElse(defParallelization);
    }

    def getSparkContext(parallelization: Int, appName: String): SparkContext = {

        JobLogger.log("Initializing Spark Context")
        val conf = new SparkConf().setAppName(appName);
        val master = conf.getOption("spark.master");
        // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered as they depend on environment variables
        if (master.isEmpty) {
            JobLogger.log("Master not found. Setting it to local[*]")
            conf.setMaster("local[*]");
        }
       
        if (!conf.contains("spark.cassandra.connection.host")) 
            conf.set("spark.cassandra.connection.host", AppConf.getConfig("spark.cassandra.connection.host"))
        if(embeddedCassandraMode)
          conf.set("spark.cassandra.connection.port", AppConf.getConfig("cassandra.service.embedded.connection.port"))
        if (!conf.contains("reactiveinflux.url")) {
            conf.set("reactiveinflux.url", AppConf.getConfig("reactiveinflux.url"));
        }
        // $COVERAGE-ON$
        val sc = new SparkContext(conf);
        setS3Conf(sc);
        JobLogger.log("Spark Context initialized");
        sc;
    }
    
    private def embeddedCassandraMode() : Boolean = {
    	val isEmbedded = AppConf.getConfig("cassandra.service.embedded.enable");
    	StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded);
    }
    
    def setS3Conf(sc: SparkContext) = {
        JobLogger.log("Configuring S3 AccessKey& SecrateKey to SparkContext")
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getAwsKey());
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getAwsSecret());
    }

    def closeSparkContext()(implicit sc: SparkContext) {
        JobLogger.log("Closing Spark Context")
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
        JobLogger.log("Deleting directory", Option(path.toString()))
        Files.walkFileTree(path, new Visitor());
    }

    def createDirectory(dir: String) {
        val path = get(dir);
        JobLogger.log("Creating directory", Option(path.toString()))
        Files.createDirectories(path);
    }

    def copyFile(from: InputStream, path: String, fileName: String) = {
        createDirectory(path);
        Files.copy(from, Paths.get(path + fileName), StandardCopyOption.REPLACE_EXISTING);
    }

    def deleteFile(file: String) {
        JobLogger.log("Deleting file ", Option(file))
        val path = get(file);
        if (Files.exists(path))
            Files.delete(path);
    }

    def datesBetween(from: LocalDate, to: LocalDate): IndexedSeq[LocalDate] = {
        val numberOfDays = Days.daysBetween(from, to).getDays()
        for (f <- 0 to numberOfDays) yield from.plusDays(f)
    }

    def getStartDate(endDate: Option[String], delta: Int): Option[String] = {
        val to = if (endDate.nonEmpty) dateFormat.parseLocalDate(endDate.get) else LocalDate.fromDateFields(new Date);
        Option(to.minusDays(delta).toString());
    }

    def getDatesBetween(fromDate: String, toDate: Option[String], pattern: String): Array[String] = {
        val df: DateTimeFormatter = DateTimeFormat.forPattern(pattern).withZoneUTC();
        val to = if (toDate.nonEmpty) df.parseLocalDate(toDate.get) else LocalDate.fromDateFields(new Date);
        val from = df.parseLocalDate(fromDate);
        val dates = datesBetween(from, to);
        dates.map { x => df.print(x) }.toArray;
    }

    def getDatesBetween(fromDate: String, toDate: Option[String]): Array[String] = {
        getDatesBetween(fromDate, toDate, "yyyy-MM-dd");
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
        getEventSyncTS(timeInString);
    }

    def getEventSyncTS(timeInStr: String) : Long = {
    	var ts = getTimestamp(timeInStr, df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        if (ts == 0) {
            ts = getTimestamp(timeInStr, df3, "yyyy-MM-dd'T'HH:mm:ssZZ");
        }
        if (ts == 0) {
            try {
                ts = getTimestamp(timeInStr.substring(0, 19), df6, "yyyy-MM-dd'T'HH:mm:ss");
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
                JobLogger.log("Invalid event time", Option(Map("ts" -> event.ts)));
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
                JobLogger.log(e.getMessage, None, ERROR)
                throw e
        }
        path ++ ".gz";
    }

    def zip(out: String, files: Iterable[String]) = {
        import java.io.{ BufferedInputStream, FileInputStream, FileOutputStream }
        import java.util.zip.{ ZipEntry, ZipOutputStream }

        val zip = new ZipOutputStream(new FileOutputStream(out))

        files.foreach { name =>
            zip.putNextEntry(new ZipEntry(name.split("/").last))
            val in = new BufferedInputStream(new FileInputStream(name))
            var b = in.read()
            while (b > -1) {
                zip.write(b)
                b = in.read()
            }
            in.close()
            zip.closeEntry()
        }
        zip.close()
    }

    def zipFolder(outFile: String, dir: String) = {
        import java.io.{ BufferedInputStream, FileInputStream, FileOutputStream }
        import java.util.zip.{ ZipEntry, ZipOutputStream }

        val zip = new ZipOutputStream(new FileOutputStream(outFile))
        val files = new File(dir).listFiles();
        files.foreach { file =>
            zip.putNextEntry(new ZipEntry(file.getName.split("/").last))
            val in = new BufferedInputStream(new FileInputStream(file))
            var b = in.read()
            while (b > -1) {
                zip.write(b)
                b = in.read()
            }
            in.close()
            zip.closeEntry()
        }
        zip.close()
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
                JobLogger.log("Invalid time format", Option(Map("pattern" -> pattern, "ts" -> ts)));
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
        val tags = metadata.getOrElse("tags", List[String]());
        if (null == tags) Option(Array[String]()) else Option(tags.asInstanceOf[List[String]].toArray);
    }

    def roundDouble(value: Double, precision: Int): Double = {
        BigDecimal(value).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble;
    }

    def getMessageId(eventId: String, userId: String, granularity: String, dateRange: DtRange, contentId: String = "NA"): String = {
        val key = Array(eventId, userId, dateRange.from, dateRange.to, granularity, contentId).mkString("|");
        MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
    }

    def getMessageId(eventId: String, userId: String, granularity: String, syncDate: Long): String = {
        val key = Array(eventId, userId, dateFormat.print(syncDate), granularity).mkString("|");
        MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
    }
    
    def getMessageId(eventId: String, level: String, timeStamp: Long): String = {
        val key = Array(eventId, level, df5.print(timeStamp)).mkString("|");
        MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
    }

    def getPeriod(date: DateTime, period: Period): Int = {
        getPeriod(date.getMillis, period);
    }

    def getPeriod(syncts: Long, period: Period): Int = {
        val d = new DateTime(syncts, DateTimeZone.UTC);
        period match {
            case DAY        => dayPeriod.print(d).toInt;
            case WEEK       => getWeekNumber(d.getWeekyear, d.getWeekOfWeekyear)
            case MONTH      => monthPeriod.print(d).toInt;
            case CUMULATIVE => 0
            case LAST7      => 7
            case LAST30     => 30
            case LAST90     => 90
        }
    }

    def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
        val from = new LocalDate(fromDate, DateTimeZone.UTC)
        val to = new LocalDate(toDate, DateTimeZone.UTC)
        Weeks.weeksBetween(from, to).getWeeks;
    }

    private def getWeekNumber(year: Int, weekOfWeekyear: Int): Int = {

        if (weekOfWeekyear < 10) {
            (year + "70" + weekOfWeekyear).toInt
        } else {
            (year + "7" + weekOfWeekyear).toInt
        }
    }

    def time[R](block: => R): (Long, R) = {
        val t0 = System.currentTimeMillis()
        val result = block // call-by-name
        val t1 = System.currentTimeMillis()
        ((t1 - t0), result)
    }

    def getPathFromURL(absUrl: String): String = {
        val url = new URL(absUrl);
        url.getPath
    }

    @throws(classOf[Exception])
    def getPeriods(period: Period, periodUpTo: Int): Array[Int] = {
        period match {
            case DAY        => getDayPeriods(periodUpTo);
            case MONTH      => getMonthPeriods(periodUpTo);
            case WEEK       => getWeekPeriods(periodUpTo);
            case CUMULATIVE => Array(0);
        }
    }

    def getPeriods(periodType: String, periodUpTo: Int): Array[Int] = {
        Period.withName(periodType) match {
            case DAY        => getDayPeriods(periodUpTo);
            case MONTH      => getMonthPeriods(periodUpTo);
            case WEEK       => getWeekPeriods(periodUpTo);
            case CUMULATIVE => Array(0);
        }
    }

    private def getDayPeriods(count: Int): Array[Int] = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val x = for (i <- 1 to count) yield getPeriod(endDate.minusDays(i), DAY);
        x.toArray;
    }

    private def getMonthPeriods(count: Int): Array[Int] = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val x = for (i <- 0 to (count - 1)) yield getPeriod(endDate.minusMonths(i), MONTH);
        x.toArray;
    }

    private def getWeekPeriods(count: Int): Array[Int] = {
        val endDate = DateTime.now(DateTimeZone.UTC);
        val x = for (i <- 0 to (count - 1)) yield getPeriod(endDate.minusWeeks(i), WEEK);
        x.toArray;
    }

    def getValidTags(event: DerivedEvent, registeredTags: Array[String]): Array[String] = {

        val tagList = event.tags.get.asInstanceOf[List[Map[String, List[String]]]]
        val genieTagFilter = if (tagList.nonEmpty) tagList.filter(f => f.contains("genie")) else List()
        val tempList = if (genieTagFilter.nonEmpty) genieTagFilter.filter(f => f.contains("genie")).last.get("genie").get; else List();
        tempList.filter { x => registeredTags.contains(x) }.toArray;
    }

    def caseClassToMap(ccObj: AnyRef) =
        (Map[String, AnyRef]() /: ccObj.getClass.getDeclaredFields) {
            (map, field) =>
                field.setAccessible(true)
                map + (field.getName -> field.get(ccObj))
        }

    def caseClassToMapWithDateConversion(ccObj: AnyRef) =
        (Map[String, AnyRef]() /: ccObj.getClass.getDeclaredFields) {
            (map, field) =>
                field.setAccessible(true)
                map + (field.getName -> (if(field.get(ccObj).isInstanceOf[DateTime]) field.get(ccObj).asInstanceOf[DateTime].getMillis.asInstanceOf[AnyRef] else field.get(ccObj)))
        }

    def getEndTimestampOfDay(date: String): Long = {
        val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
        dateFormat.parseDateTime(date).plusHours(23).plusMinutes(59).plusSeconds(59).getMillis
    }
}