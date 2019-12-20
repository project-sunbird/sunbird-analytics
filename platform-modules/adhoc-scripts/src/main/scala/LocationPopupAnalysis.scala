import java.text.NumberFormat

import scala.reflect.ManifestFactory.classType

import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.toRDDFunctions
import org.ekstep.analytics.framework.FrameworkContext

//case class DeviceProfileId(device_id: String)
//case class DeviceProfile(device_id: String, first_access: Option[Long], state_custom: Option[String], district_custom: Option[String], user_declared_state: Option[String], user_declared_district: Option[String])
//case class DeviceProfileOutput(userDeclared: Int, userUpdated: Int, userUpdatedState: Int, userUpdatedDist: Int)

object LocationPopupAnalysis extends optional.Application {

  def deviceStats(dp: DeviceProfile): DeviceProfileOutput = {
    val userDeclared = if (dp.user_declared_state.nonEmpty) 1 else 0;
    val userUpdatedState = if (userDeclared == 1 && !dp.user_declared_state.get.equalsIgnoreCase(dp.state_custom.getOrElse(""))) 1 else 0;
    val userUpdatedDist = if (userDeclared == 1 && !dp.user_declared_district.getOrElse("").equalsIgnoreCase(dp.district_custom.getOrElse(""))) 1 else 0;
    val userUpdated = if (userDeclared == 1 && (userUpdatedState == 1 || userUpdatedDist == 1)) 1 else 0;
    DeviceProfileOutput(userDeclared, userUpdated, userUpdatedState, userUpdatedDist);
  }

  def percent(v1: Int, v2: Int): String = {
    NumberFormat.getPercentInstance.format(v2.toDouble / v1.toDouble)
  }

  def percentDiff(v1: Int, v2: Int): String = {
    NumberFormat.getPercentInstance.format((v1.toDouble - v2.toDouble) / v1)
  }

  def main(date: String, cassandraIp: String): Unit = {

    //val queryConfig = """{"type":"local","queries":[{"file":"/Users/Santhosh/EkStep/telemetry/unique/2019-12-11-1576076333659.json"}]}""";
    val queryConfig = s"""{"type":"azure","queries":[{"bucket": "telemetry-data-store", "prefix": "unique/", "endDate": "$date","delta": 0}]}""";
    implicit val spark = CommonUtil.getSparkSession(10, "LocationPopupAnalysis", Option(cassandraIp));
    implicit val sparkContext = spark.sparkContext;
    implicit val fc = new FrameworkContext();
    val data = DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig));
    val deviceData = data.filter(f => "sunbird.app".equals(f.context.pdata.get.pid.getOrElse(""))).groupBy(f => f.context.did.get)
    val devicesWithPopup = deviceData.mapValues(f => {
      val firstTime = f.filter(p => "IMPRESSION".equals(p.eid) && "onboarding-language-setting".equals(p.edata.pageid));
      val locationPopupViews = f.filter(p => "IMPRESSION".equals(p.eid) && "district-mapping".equals(p.edata.pageid));
      val locationSubmits = f.filter(p => "INTERACT".equals(p.eid) && "district-mapping".equals(p.edata.pageid) && "submit-clicked".equals(p.edata.subtype))
      (if (firstTime.size > 0) 1 else 0, if (locationPopupViews.size > 0) 1 else 0, if (locationSubmits.size > 0) 1 else 0)
    }).cache();

    val totalDevices = devicesWithPopup.count();
    val newDevices = devicesWithPopup.filter(f => f._2._1 == 1);
    val oldDevices = devicesWithPopup.filter(f => f._2._1 == 0);

    val newDevicesStats = newDevices.map(f => f._2).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    val oldDevicesStats = oldDevices.map(f => f._2).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    val newDeviceProfiles = newDevices.map(f => DeviceProfileId(f._1)).joinWithCassandraTable[DeviceProfile]("ntpprod_device_db", "device_profile");
    val newDevicesLocationStats = newDeviceProfiles.map(f => deviceStats(f._2)).reduce((a, b) => DeviceProfileOutput(a.userDeclared + b.userDeclared, a.userUpdated + b.userUpdated, a.userUpdatedState + b.userUpdatedState, a.userUpdatedDist + b.userUpdatedDist))

    val oldDeviceProfiles = oldDevices.map(f => DeviceProfileId(f._1)).joinWithCassandraTable[DeviceProfile]("ntpprod_device_db", "device_profile");
    val oldDevicesLocationStats = oldDeviceProfiles.map(f => deviceStats(f._2)).reduce((a, b) => DeviceProfileOutput(a.userDeclared + b.userDeclared, a.userUpdated + b.userUpdated, a.userUpdatedState + b.userUpdatedState, a.userUpdatedDist + b.userUpdatedDist))

    print(date, totalDevices.toInt, newDevicesStats, oldDevicesStats, newDevicesLocationStats, oldDevicesLocationStats)
    sparkContext.stop();
  }

  def print(date: String, totalDevices: Int, newDevicesStats: Tuple3[Int, Int, Int], oldDevicesStats: Tuple3[Int, Int, Int], newDevicesLocationStats: DeviceProfileOutput, oldDevicesLocationStats: DeviceProfileOutput)(implicit spark: SparkSession) = {

    val data = List(
      ("New", date, newDevicesStats._1, newDevicesStats._2, newDevicesStats._3, percentDiff(newDevicesStats._2, newDevicesStats._3), newDevicesLocationStats.userDeclared,
        newDevicesLocationStats.userUpdated, newDevicesLocationStats.userUpdatedState, newDevicesLocationStats.userUpdatedDist,
        percent(newDevicesLocationStats.userDeclared, newDevicesLocationStats.userUpdated), percent(newDevicesLocationStats.userDeclared, newDevicesLocationStats.userUpdatedState),
        percent(newDevicesLocationStats.userDeclared, newDevicesLocationStats.userUpdatedDist)),
      ("Updated", date, totalDevices - newDevicesStats._1, oldDevicesStats._2, oldDevicesStats._3, percentDiff(oldDevicesStats._2, oldDevicesStats._3), oldDevicesLocationStats.userDeclared,
        oldDevicesLocationStats.userUpdated, oldDevicesLocationStats.userUpdatedState, oldDevicesLocationStats.userUpdatedDist,
        percent(oldDevicesLocationStats.userDeclared, oldDevicesLocationStats.userUpdated), percent(oldDevicesLocationStats.userDeclared, oldDevicesLocationStats.userUpdatedState),
        percent(oldDevicesLocationStats.userDeclared, oldDevicesLocationStats.userUpdatedDist)))
    val df = spark.createDataFrame(data).toDF("Type", "Date", "No of Devices", "Location Shown", "Location Submitted  ", "% Drop", "User Declared", "Updated recommendation", "Updated State", "Updated District", "% Modified", "% Modified State", "% Modified District")
    df.show();
  }

}
