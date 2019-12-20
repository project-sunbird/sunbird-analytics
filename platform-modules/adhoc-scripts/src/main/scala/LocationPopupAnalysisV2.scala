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
import org.apache.spark.SparkContext

case class DeviceProfileId(device_id: String)
case class DeviceProfile(device_id: String, first_access: Option[Long], state_custom: Option[String], district_custom: Option[String], user_declared_state: Option[String], user_declared_district: Option[String])
case class DeviceProfileOutput(userDeclared: Int, userUpdated: Int, userUpdatedState: Int, userUpdatedDist: Int)
case class LocationAnalysisOutput(date: String, noOfDevices: Int, locShown: Int, locSubmitted: Int, userDeclared: Int)

object LocationPopupAnalysisV2 extends optional.Application {

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

  def main(from: String, to: String, cassandraIp: String): Unit = {
    
    implicit val spark = CommonUtil.getSparkSession(10, "LocationPopupAnalysis", Option(cassandraIp));
    implicit val sparkContext = spark.sparkContext;
    implicit val fc = new FrameworkContext();
    val dates = CommonUtil.getDatesBetween(from, Option(to)).toSeq;

    Console.println("Dates", dates);
    val data = for (date <- dates) yield {
      execute(date, cassandraIp);
    }
    val df = spark.createDataFrame(data).toDF("Date", "No of Devices", "Location Shown", "Location Submitted", "User Declared")
    df.show();
    sparkContext.stop();
  }

  def execute(date: String, cassandraIp: String)(implicit sc: SparkContext, fc: FrameworkContext): LocationAnalysisOutput = {

    //val queryConfig = """{"type":"local","queries":[{"file":"/Users/Santhosh/EkStep/telemetry/unique/2019-12-11-1576076333659.json"}]}""";
    val queryConfig = s"""{"type":"azure","queries":[{"bucket": "telemetry-data-store", "prefix": "unique/", "endDate": "$date","delta": 0}]}""";
    val data = DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig));
    val deviceData = data.filter(f => f.context.pdata.nonEmpty).filter(f => "sunbird.app".equals(f.context.pdata.get.pid.getOrElse(""))).filter(f => f.context.pdata.get.ver.getOrElse("").startsWith("2.5")).groupBy(f => f.context.did.get)
    val devicesWithPopup = deviceData.mapValues(f => {
      val firstTime = f.filter(p => "IMPRESSION".equals(p.eid) && "onboarding-language-setting".equals(p.edata.pageid));
      val locationPopupViews = f.filter(p => "IMPRESSION".equals(p.eid) && "district-mapping".equals(p.edata.pageid));
      val locationSubmits = f.filter(p => "INTERACT".equals(p.eid) && "district-mapping".equals(p.edata.pageid) && "submit-clicked".equals(p.edata.subtype))
      (if (firstTime.size > 0) 1 else 0, if (locationPopupViews.size > 0) 1 else 0, if (locationSubmits.size > 0) 1 else 0)
    }).cache();

    val newDevices = devicesWithPopup.filter(f => f._2._1 == 1);

    val newDevicesStats = newDevices.map(f => f._2).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    val newDeviceProfiles = newDevices.map(f => DeviceProfileId(f._1)).joinWithCassandraTable[DeviceProfile]("ntpprod_device_db", "device_profile");
    val newDevicesLocationStats = newDeviceProfiles.map(f => deviceStats(f._2)).reduce((a, b) => DeviceProfileOutput(a.userDeclared + b.userDeclared, a.userUpdated + b.userUpdated, a.userUpdatedState + b.userUpdatedState, a.userUpdatedDist + b.userUpdatedDist))

    devicesWithPopup.unpersist(true);
    LocationAnalysisOutput(date, newDevicesStats._1, newDevicesStats._2, newDevicesStats._3, newDevicesLocationStats.userDeclared);
  }
  
}
