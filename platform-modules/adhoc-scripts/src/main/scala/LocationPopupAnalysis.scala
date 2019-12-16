import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.FrameworkContext
import scala.reflect.ManifestFactory.classType
import org.ekstep.analytics.framework.V3Event
import com.datastax.spark.connector._

case class DeviceProfileId(device_id: String)
case class DeviceProfile(device_id: String, first_access: Option[Long], state_custom: Option[String], district_custom: Option[String], user_declared_state: Option[String], user_declared_district: Option[String])
case class DeviceProfileOutput(userDeclared: Int, userUpdated: Int, userUpdatedState: Int, userUpdatedDist: Int)

object LocationPopupAnalysis extends optional.Application {

  def deviceStats(dp: DeviceProfile): DeviceProfileOutput = {
      val userDeclared = if(dp.user_declared_state.nonEmpty) 1 else 0;
      val userUpdatedState = if(userDeclared == 1 && !dp.user_declared_state.get.equalsIgnoreCase(dp.state_custom.getOrElse(""))) 1 else 0;
      val userUpdatedDist = if(userDeclared == 1 && !dp.user_declared_district.getOrElse("").equalsIgnoreCase(dp.district_custom.getOrElse(""))) 1 else 0;
      val userUpdated = if(userDeclared == 1 && (userUpdatedState == 1 || userUpdatedDist == 1)) 1 else 0;
      DeviceProfileOutput(userDeclared, userUpdated, userUpdatedState, userUpdatedDist);
  }
  
  def main(date: String, cassandraIp: String): Unit = {
    
    //val queryConfig = """{"type":"local","queries":[{"file":"/Users/Santhosh/EkStep/telemetry/unique/2019-12-11-1576076333659.json"}]}""";
    val queryConfig = s"""{"type":"azure","queries":[{"bucket": "telemetry-data-store", "prefix": "unique/", "endDate": "$date","delta": 0}]}""";
    implicit val sparkContext = CommonUtil.getSparkContext(10, "LocationPopupAnalysis", Option(cassandraIp));
    implicit val fc = new FrameworkContext();
    val data = DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig));
    val deviceData = data.filter(f => "sunbird.app".equals(f.context.pdata.get.pid.getOrElse(""))).groupBy(f => f.context.did.get)
    val devicesWithPopup = deviceData.mapValues(f => {
      val firstTime = f.filter(p => "IMPRESSION".equals(p.eid) && "onboarding-language-setting".equals(p.edata.pageid));
      val locationPopupViews = f.filter(p => "IMPRESSION".equals(p.eid) && "district-mapping".equals(p.edata.pageid));
      val locationSubmits = f.filter(p => "INTERACT".equals(p.eid) && "district-mapping".equals(p.edata.pageid) && "submit-clicked".equals(p.edata.subtype))
      (if(firstTime.size > 0) 1 else 0, if(locationPopupViews.size > 0)  1 else 0, if(locationSubmits.size > 0)  1 else 0)
    }).cache();
    
    val totalDevices = devicesWithPopup.count();
    val newDevices = devicesWithPopup.filter(f => f._2._1 == 1);
    val oldDevices = devicesWithPopup.filter(f => f._2._1 == 0)
    //newDevices.take(4).foreach(f => println(f._1))
    //oldDevices.take(4).foreach(f => println(f._1))
    
    val newDeviceProfiles = newDevices.map(f => DeviceProfileId(f._1)).joinWithCassandraTable[DeviceProfile]("ntpprod", "device_profile");
    val newDevicesStats = newDeviceProfiles.map(f => deviceStats(f._2)).reduce((a, b) => DeviceProfileOutput(a.userDeclared + b.userDeclared, a.userUpdated + b.userUpdated, a.userUpdatedState + b.userUpdatedState, a.userUpdatedDist + b.userUpdatedDist))
    
    val oldDeviceProfiles = oldDevices.map(f => DeviceProfileId(f._1)).joinWithCassandraTable[DeviceProfile]("ntpprod", "device_profile");
    val oldDevicesStats = oldDeviceProfiles.map(f => deviceStats(f._2)).reduce((a, b) => DeviceProfileOutput(a.userDeclared + b.userDeclared, a.userUpdated + b.userUpdated, a.userUpdatedState + b.userUpdatedState, a.userUpdatedDist + b.userUpdatedDist))
    
    Console.println(date, "totalDevices", totalDevices);
    Console.println(date, "new device stats", newDevicesStats);
    Console.println(date, "old device stats", oldDevicesStats);
    sparkContext.stop();
  }

}
