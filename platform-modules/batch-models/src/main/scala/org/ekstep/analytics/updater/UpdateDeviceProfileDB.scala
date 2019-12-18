package org.ekstep.analytics.updater

import java.sql.Timestamp

import org.apache.spark.rdd._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, PostgresDBUtil}
import scala.collection.mutable.Buffer

case class DeviceProfileKey(device_id: String)
case class DeviceProfileInput(index: DeviceProfileKey, currentData: Buffer[DerivedEvent], previousData: Option[DeviceProfileOutput]) extends AlgoInput
object UpdateDeviceProfileDB extends IBatchModelTemplate[DerivedEvent, DeviceProfileInput, DeviceProfileOutput, Empty] with Serializable {

  val className = "org.ekstep.analytics.model.UpdateDeviceProfileDB"

  override def name: String = "UpdateDeviceProfileDB"

  val postgreDB = new PostgresDBUtil()
  val table = AppConf.getConfig("postgres.device.table_name")

  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DeviceProfileInput] = {

    val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_DEVICE_SUMMARY")))
    val newGroupedEvents = filteredEvents.map { event =>
      (DeviceProfileKey(event.dimensions.did.get), Buffer(event))
    }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);

    val query = s"SELECT * FROM $table"
    val responseRDD = sc.parallelize(postgreDB.readDBData(query))

    val newEvents = newGroupedEvents.map(f => (DeviceProfileKey(f._1.device_id), f))
    val prevDeviceProfile = responseRDD.map(f => (DeviceProfileKey(f.device_id), f))

    val deviceData = newEvents.leftOuterJoin(prevDeviceProfile)
    deviceData.map { x =>
      val deviceProfileInputData = x._2
      DeviceProfileInput(deviceProfileInputData._1._1, deviceProfileInputData._1._2, deviceProfileInputData._2)
    }
  }

  override def algorithm(data: RDD[DeviceProfileInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DeviceProfileOutput] = {
    data.map { events =>
      val eventsSortedByFromDate = events.currentData.sortBy { x => x.context.date_range.from };
      val eventsSortedByToDate = events.currentData.sortBy { x => x.context.date_range.to };
      val prevProfileData = events.previousData.getOrElse(DeviceProfileOutput(events.index.device_id, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None));
      val eventStartTime = new Timestamp(eventsSortedByFromDate.head.context.date_range.from)
      val first_access = if (prevProfileData.first_access.isEmpty) eventStartTime else if (eventStartTime.getTime > prevProfileData.first_access.get.getTime) prevProfileData.first_access.get else eventStartTime;
      val eventEndTime = new Timestamp(eventsSortedByToDate.last.context.date_range.to)
      val last_access = if (prevProfileData.last_access.isEmpty) eventEndTime else if (eventEndTime.getTime < prevProfileData.last_access.get.getTime) prevProfileData.last_access.get else eventEndTime;
      val current_ts = events.currentData.map { x =>
        (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("total_ts").get.asInstanceOf[Double])
      }.sum
      val total_ts = if (prevProfileData.total_ts.isEmpty) current_ts else current_ts + prevProfileData.total_ts.get
      val current_launches = events.currentData.map { x =>
        (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("total_launches").get.asInstanceOf[Number].longValue())
      }.sum
      val total_launches = if (prevProfileData.total_launches.isEmpty) current_launches else current_launches + prevProfileData.total_launches.get
      val avg_ts = if (total_launches == 0) total_ts else CommonUtil.roundDouble(total_ts / total_launches, 2)
      DeviceProfileOutput(events.index.device_id, Option(first_access), Option(last_access), Option(total_ts), Option(total_launches), Option(avg_ts), prevProfileData.device_spec, prevProfileData.uaspec, prevProfileData.state, prevProfileData.city, prevProfileData.country, prevProfileData.country_code, prevProfileData.state_code, prevProfileData.state_custom, prevProfileData.state_code_custom, prevProfileData.district_custom, prevProfileData.fcm_token, prevProfileData.producer_id, prevProfileData.user_declared_state, prevProfileData.user_declared_district, prevProfileData.api_last_updated_on)
    }
  }

  override def postProcess(data: RDD[DeviceProfileOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    data.foreach { f =>
      val query =
        s"""INSERT INTO $table (device_id, first_access, last_access, total_ts, total_launches, avg_ts, device_spec, uaspec, state,
           |city, country, country_code, state_code, state_custom, state_code_custom, district_custom, fcm_token, producer_id,
           |user_declared_state, user_declared_district, api_last_updated_on, updated_date) VALUES
           |('${f.device_id}', '${f.first_access.get}', '${f.last_access.get}', '${f.total_ts.getOrElse(0)}',
           |'${f.total_launches.getOrElse("")}', '${f.avg_ts.getOrElse(0)}', '${JSONUtils.serialize(f.device_spec.getOrElse(""))}',
           |'${JSONUtils.serialize(f.uaspec.getOrElse(""))}', '${f.state.getOrElse("")}', '${f.city.getOrElse("")}', '${f.country.getOrElse("")}',
           |'${f.country_code.getOrElse("")}', '${f.state_code.getOrElse("")}', '${f.state_custom.getOrElse("")}',
           |'${f.state_code_custom.getOrElse("")}','${f.district_custom.getOrElse("")}', '${f.fcm_token.getOrElse("")}',
           |'${f.producer_id.getOrElse("")}', '${f.user_declared_state.getOrElse("")}', '${f.user_declared_district.getOrElse("")}',
           |'${f.api_last_updated_on.getOrElse(new Timestamp(System.currentTimeMillis()))}', '${f.updated_date.get}')
           |ON CONFLICT (device_id) DO
           |UPDATE SET device_id = '${f.device_id}', first_access = '${f.first_access.get}', last_access = '${f.last_access.get}',
           |total_ts = '${f.total_ts.getOrElse(0)}', total_launches = '${f.total_launches.getOrElse("")}', avg_ts= '${f.avg_ts.getOrElse(0)}',
           |device_spec = '${JSONUtils.serialize(f.device_spec.getOrElse(""))}', uaspec = '${JSONUtils.serialize(f.uaspec.getOrElse(""))}',
           |state = '${f.state.getOrElse("")}', city = '${f.city.getOrElse("")}', country = '${f.country.getOrElse("")}', country_code= '${f.country_code.getOrElse("")}',
           |state_code = '${f.state_code.getOrElse("")}', state_custom = '${f.state_custom.getOrElse("")}', state_code_custom = '${f.state_code_custom.getOrElse("")}',
           |district_custom = '${f.district_custom.getOrElse("")}', fcm_token = '${f.fcm_token.getOrElse("")}', producer_id = '${f.producer_id.getOrElse("")}',
           |user_declared_state = '${f.user_declared_state.getOrElse("")}', user_declared_district = '${f.user_declared_district.getOrElse("")}',
           |api_last_updated_on = '${f.api_last_updated_on.getOrElse(new Timestamp(System.currentTimeMillis()))}', updated_date = '${f.updated_date.get}'""".stripMargin
      postgreDB.insertDataToPostgresDB(query)
    }
    sc.makeRDD(List(Empty()));
  }
}