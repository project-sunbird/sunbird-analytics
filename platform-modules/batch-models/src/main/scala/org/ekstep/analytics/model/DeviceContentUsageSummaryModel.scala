package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.joda.time.DateTime

case class DeviceSummaryInput(device_id: String, data: Buffer[DerivedEvent], prevData: Option[DeviceUsageSummary]) extends AlgoInput
case class DeviceContentUsageSummaryInput(device_id: String, contentId: String, data: Buffer[DerivedEvent], prevData: Option[DeviceContentSummary])
case class DeviceContentSummary(device_id: String, content_id: String, game_ver: Option[String], num_sessions: Option[Long], total_interactions: Option[Long], avg_interactions_min: Option[Double],
                                total_timespent: Option[Double], last_played_on: Option[Long], start_time: Option[Long],
                                mean_play_time_interval: Option[Double], downloaded: Option[Boolean], download_date: Option[Long], num_group_user: Option[Long], num_individual_user: Option[Long], updated_date: DateTime = DateTime.now()) extends AlgoOutput with Output;
case class DeviceContentSummaryIndex(device_id: String, content_id: String)

object DeviceContentUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, DeviceSummaryInput, DeviceContentSummary, DeviceContentSummary] with Serializable {

    val className = "org.ekstep.analytics.model.DeviceContentUsageSummaryModel"
    override def name: String = "DeviceContentUsageSummaryModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceSummaryInput] = {

        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        val deviceSessions = filteredEvents.map { event =>
            val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val did = event.dimensions.did.get
            (did, Buffer(event));
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        val prevDeviceSummary = deviceSessions.map(f => DeviceId(f._1)).joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map(f => (f._1.device_id, f._2))
        val joinedData = deviceSessions.leftOuterJoin(prevDeviceSummary)
        joinedData.map(f => DeviceSummaryInput(f._1, f._2._1, f._2._2));
    }

    override def algorithm(data: RDD[DeviceSummaryInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContentSummary] = {

        val deviceDetails = data.map { deviceSummary =>
            val prevSummary = deviceSummary.prevData.getOrElse(DeviceUsageSummary(deviceSummary.device_id, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None));
            val events = deviceSummary.data
            val firstEvent = events.sortBy { x => x.context.date_range.from }.head;
            val lastEvent = events.sortBy { x => x.context.date_range.to }.last;
            
            val eventStartTime = firstEvent.context.date_range.from
            val play_start_time = if (prevSummary.play_start_time.isEmpty) eventStartTime else if (eventStartTime > prevSummary.play_start_time.get) prevSummary.play_start_time.get else eventStartTime
            val last_played_on = lastEvent.context.date_range.to
            val current_play_time = CommonUtil.roundDouble(events.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double]) }.sum, 2)
            val total_play_time = if (prevSummary.total_play_time.isEmpty) current_play_time else current_play_time + prevSummary.total_play_time.get;
            val num_sessions = if (prevSummary.num_sessions.isEmpty) events.size else events.size + prevSummary.num_sessions.get
            val mean_play_time = CommonUtil.roundDouble(total_play_time / num_sessions, 2)
            val timeDiff = CommonUtil.getTimeDiff(play_start_time, last_played_on).get
            val play_time_interval = timeDiff - total_play_time
            val mean_play_time_interval = if (num_sessions < 2) 0d else CommonUtil.roundDouble(BigDecimal(play_time_interval / (num_sessions - 1)).toDouble, 2)
            val last_played_content = lastEvent.dimensions.gdata.get.id
            (prevSummary.device_id, Option(play_start_time), Option(last_played_on), Option(total_play_time), Option(num_sessions), Option(mean_play_time), Option(mean_play_time_interval), Option(last_played_content))
        }
        deviceDetails.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE, SomeColumns("device_id","play_start_time","last_played_on","total_play_time","num_sessions","mean_play_time","mean_play_time_interval","last_played_content"))

        val inputEvents = data.flatMap { x => x.data }
        val dcuSummaries = inputEvents.map { event =>
            val did = event.dimensions.did.get
            val content_id = event.dimensions.gdata.get.id
            ((did, content_id), Buffer(event));
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        val prevDeviceContentSummary = dcuSummaries.map(f => DeviceContentSummaryIndex(f._1._1, f._1._2)).joinWithCassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT).on(SomeColumns("device_id", "content_id")).map(f => ((f._1.device_id, f._1.content_id), f._2))
        val joinedData = dcuSummaries.leftOuterJoin(prevDeviceContentSummary)
        val dcusEvents = joinedData.map(f => DeviceContentUsageSummaryInput(f._1._1, f._1._2, f._2._1, f._2._2));

        dcusEvents.map { dcusEvent =>
            val firstEvent = dcusEvent.data.sortBy { x => x.context.date_range.from }.head;
            val lastEvent = dcusEvent.data.sortBy { x => x.context.date_range.to }.last;
            val game_ver = firstEvent.dimensions.gdata.get.ver
            val prevDeviceContentSummary = dcusEvent.prevData.getOrElse(DeviceContentSummary(dcusEvent.device_id, dcusEvent.contentId, None, None, None, None, None, None, None, None, None, None, None, None))
            val num_sessions = if (prevDeviceContentSummary.num_sessions.isEmpty) dcusEvent.data.size else dcusEvent.data.size + prevDeviceContentSummary.num_sessions.get
            val current_ts = CommonUtil.roundDouble(dcusEvent.data.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double]) }.sum, 2)
            val total_timespent = if (prevDeviceContentSummary.total_timespent.isEmpty) current_ts else current_ts + prevDeviceContentSummary.total_timespent.get;
            val current_interactions = dcusEvent.data.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int] }.sum
            val total_interactions = if (prevDeviceContentSummary.total_timespent.isEmpty) current_interactions else current_interactions + prevDeviceContentSummary.total_interactions.get
            val avg_interactions_min = if (total_interactions == 0 || total_timespent == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_timespent / 60)).toDouble, 2);
            val last_played_on = lastEvent.context.date_range.to
            val eventStartTime = firstEvent.context.date_range.from
            val start_time = if (prevDeviceContentSummary.start_time.isEmpty) eventStartTime else if (eventStartTime > prevDeviceContentSummary.start_time.get) eventStartTime else prevDeviceContentSummary.start_time.get
            val timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(start_time, last_played_on).get, 2)
            val play_time_interval = timeDiff - total_timespent
            val mean_play_time_interval = if (num_sessions < 2) 0d else CommonUtil.roundDouble(BigDecimal(play_time_interval / (num_sessions - 1)).toDouble, 2)
            val downloaded = prevDeviceContentSummary.downloaded
            val download_date = prevDeviceContentSummary.download_date
            val num_group_user = dcusEvent.data.map { x => x.dimensions.group_user.get }.count { y => true.equals(y) }
            val num_individual_user = num_sessions - num_group_user
            DeviceContentSummary(dcusEvent.device_id, dcusEvent.contentId, Option(game_ver), Option(num_sessions), Option(total_interactions), Option(avg_interactions_min), Option(total_timespent), Option(last_played_on), Option(start_time), Option(mean_play_time_interval), downloaded, download_date, Option(num_group_user), Option(num_individual_user))
        }
    }

    override def postProcess(data: RDD[DeviceContentSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContentSummary] = {

        data.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT)
        data;
    }
}