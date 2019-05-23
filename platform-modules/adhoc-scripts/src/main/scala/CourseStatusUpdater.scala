import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}

case class UpdaterConfig(bucket: Option[String], prefix: Option[String], startDate: Option[String], endDate: Option[String], courseId: String, batchId: String,
                         userId: Option[String], cassandraHost: Option[String], apiToken: Option[String], mode: Option[String], file: Option[String],
                         dryrun: Option[Boolean], syncData: Option[Boolean], syncURL: Option[String], updateCourseConsumption: Option[Boolean])

case class ContentConsumption(id: String, batchid: String, contentid: String, courseid: String, status: Int, userid: String)

case class UserCourses(id: String, batchid: String, courseid: String, progress: Int, status: Int, userid: String)

object CourseStatusUpdater extends optional.Application {

  def main(config: String, isTest: Boolean) {

    val execTime = CommonUtil.time({
      val updaterConfig = JSONUtils.deserialize[UpdaterConfig](config);
      implicit val sparkContext = CommonUtil.getSparkContext(10, "CourseStatusUpdater", updaterConfig.cassandraHost)
      val data = if (updaterConfig.mode.getOrElse("local").equals("local")) getTestData(updaterConfig) else getData(updaterConfig);
      correctCourseConsumption(data, updaterConfig);
      updateUserCourses(updaterConfig);
      syncData(updaterConfig);
      sparkContext.stop
    })
    Console.println("Time taken to execute:", execTime._1);
  }

  def getData(config: UpdaterConfig)(implicit sc: SparkContext): RDD[V3Event] = {

    val queries = Option(Array(Query(config.bucket, config.prefix, config.startDate, config.endDate)));
    DataFetcher.fetchBatchData[V3Event](Fetcher("azure", None, queries))
  }

  def getTestData(config: UpdaterConfig)(implicit sc: SparkContext): RDD[V3Event] = {

    val file = config.file.get;
    val queryConfig = s"""{"type":"local","queries":[{"file":"$file"}]}""";
    DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig))
  }

  def correctCourseConsumption(data: RDD[V3Event], config: UpdaterConfig)(implicit sc: SparkContext): Unit = {

    Console.println("Correct course consumption entries....");
    val playerEndEvents = DataFilter.filter(data, Array(Filter("eid", "EQ", Option("END")))).filter(f => f.context.pdata.nonEmpty && f.context.pdata.get.pid.nonEmpty && (f.context.pdata.get.pid.get.equals("sunbird.app.contentplayer") || f.context.pdata.get.pid.get.equals("sunbird-portal.contentplayer")));
    val courseEndEvents = playerEndEvents.filter(f => {
      try {
        ((f.context != null && f.context.rollup.nonEmpty && f.context.rollup.get.l1 != null && f.context.rollup.get.l1.equals(config.courseId)) ||
          (f.`object`.nonEmpty && f.`object`.get.rollup.nonEmpty && f.`object`.get.rollup.get.l1 != null && f.`object`.get.rollup.get.l1.equals(config.courseId))) ||
          (f.context.cdata.nonEmpty && f.context.cdata.get.filter(p => p.`type`.equalsIgnoreCase("course")).size > 0 && f.context.cdata.get.filter(p => p.`type`.equalsIgnoreCase("course")).head.id.equals(config.courseId))
      } catch {
        case ex: Exception =>
          Console.println("Failed Event - " + JSONUtils.serialize(f))
          throw ex;
      }

    }).filter(f => f.`object`.nonEmpty).groupBy(f => (f.actor.id, f.`object`.get.id)).mapValues(f => f.size);
    val contentConsumptionRecords = courseEndEvents.map(f => {
      ContentConsumption(hash(Array(f._1._1, f._1._2, config.courseId, config.batchId)), config.batchId, f._1._2, config.courseId, 2, f._1._1);
    }).cache()
    if (config.dryrun.getOrElse(true)) {
      Console.println();
      contentConsumptionRecords.collect().foreach(f => Console.println(f));
    } else {
      Console.println("Updating course consumption records - " + contentConsumptionRecords.count())
      contentConsumptionRecords.saveToCassandra("sunbird", "content_consumption", SomeColumns("id", "batchid", "contentid", "courseid", "status", "userid"))
    }

  }

  def updateUserCourses(config: UpdaterConfig)(implicit sc: SparkContext): Unit = {

    Console.println("Correct user courses entries....")
    val consumptionRows = sc.cassandraTable("sunbird", "content_consumption").select("userid", "contentid").where("courseid = ? and batchid = ? and status = ?", config.courseId, config.batchId, 2);
    val consumptionRecords = consumptionRows.map(f => (f.getString("userid"), f.getString("contentid")));
    val userCourses = consumptionRecords.groupBy(f => (f._1)).mapValues(f => f.size).map(f => {
      UserCourses(hash(Array(f._1, config.courseId, config.batchId)), config.batchId, config.courseId, f._2, 0, f._1);
    }).joinWithCassandraTable("sunbird", "user_courses", SomeColumns("leafnodescount", "status"), SomeColumns("id")).map(f => {
      val status = if (f._2.getInt("leafnodescount") == f._1.progress) 2 else f._2.getInt("status");
      UserCourses(f._1.id, f._1.batchid, f._1.courseid, f._1.progress, status, f._1.userid)
    }).cache();
    if (config.dryrun.getOrElse(true)) {
      Console.println()
      userCourses.collect().foreach(f => Console.println(f));
    } else {
      Console.println("Updating user courses records - " + userCourses.count())
      userCourses.saveToCassandra("sunbird", "user_courses", SomeColumns("id", "batchid", "courseid", "status", "progress", "userid"))
    }
  }

  def syncData(config: UpdaterConfig): Unit = {

    if (config.syncData.getOrElse(false)) {
      Console.println("Invoking sync api...");
      val headers = Option(Map("Content-Type" -> "application/json", "Authorization" -> ("Bearer " + config.apiToken.get)));
      val body = """{"request":{"objectType":"user_course","objectIds":["userids"]}}""";
      val response = RestUtil.post[String](config.syncURL.get + "/api/data/v1/index/sync", body, headers)
      Console.println("Response from sync api" + response);
    }

  }

  def hash(keys: Array[String]): String = {
    val key = keys.mkString("##");
    val md: MessageDigest = MessageDigest.getInstance("SHA-256");
    md.update(key.getBytes(StandardCharsets.UTF_8));
    val bytes: Array[Byte] = md.digest();
    val sb = new StringBuilder();
    for (b <- bytes) {
      sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
    }
    sb.toString();
  }

}

object TestCourseUpdater {

  def main(args: Array[String]): Unit = {
    val config = """{"courseId":"do_0127285352081571841","batchId":"0127285352081571841","file":"/Users/santhosh/EkStep/telemetry/end_events.json","syncData":true,"dryrun":false,"syncURL":"https://dev.sunbirded.org","apiToken":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkMTc1MDIwNDdlODc0ODZjOTM0ZDQ1ODdlYTQ4MmM3MyJ9.7LWocwCn5rrCScFQYOne8_Op2EOo-xTCK5JCFarHKSs"}""";
    CourseStatusUpdater.main(config, true);
  }

}