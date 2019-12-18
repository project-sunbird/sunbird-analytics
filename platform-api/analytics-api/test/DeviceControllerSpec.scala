
import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe}
import com.typesafe.config.Config
import controllers.DeviceController
import org.ekstep.analytics.api.service.{DeviceProfileService, DeviceRegisterService, ExperimentAPIService, SaveMetricsActor}
import org.ekstep.analytics.api.util.{H2DBUtil, PostgresDBUtil, RedisUtil}
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration

import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class DeviceControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {
  implicit val system = ActorSystem()
  private val configMock = mock[Config]
  private val configurationMock = mock[Configuration]
  private val redisUtilMock = mock[RedisUtil]
  private val H2DBMock = mock[H2DBUtil]
  when(configMock.getString("postgres.table.geo_location_city.name")).thenReturn("geo_location_city")
  when(configMock.getString("postgres.table.geo_location_city_ipv4.name")).thenReturn("geo_location_city_ipv4")
  when(configMock.getBoolean("device.api.enable.debug.log")).thenReturn(true)
  val saveMetricsActor = TestActorRef(new SaveMetricsActor)
  private val postgresDBMock = mock[PostgresDBUtil]
  val metricsActorProbe = TestProbe()


  "DeviceController" should "Should return success status when code is OK " in {

    val deviceRegisterServiceActorRef = TestActorRef(new DeviceRegisterService(saveMetricsActor, configMock, redisUtilMock, postgresDBMock, H2DBMock) {
      override val metricsActor = metricsActorProbe.ref
    })

    val deviceProfileServiceActorRef = TestActorRef(new DeviceProfileService(configMock, redisUtilMock, H2DBMock) {

    })

    val expActorRef = TestActorRef(new ExperimentAPIService() {
    })
    val controller = new DeviceController(deviceRegisterServiceActorRef, deviceProfileServiceActorRef, expActorRef, system, configurationMock, null, null)
    val result = controller.sendExperimentData(Option("exp1"), Option("user1"), Option("http://test/exp"), Option("yes"))
    import scala.util.{Failure, Success}
    result.onComplete {
      case Success(value) => {
        println(s"Got the callback, = $value")
      }
      case Failure(e) => {
        println(e.printStackTrace)
      }
      case _ => println("nothing")
    }
  }
}

