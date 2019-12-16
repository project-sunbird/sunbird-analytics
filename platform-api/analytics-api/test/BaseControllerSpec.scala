

import controllers.BaseController
import junit.framework.Assert
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner
import play.api.mvc.Result


@RunWith(classOf[JUnitRunner])
class BaseControllerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {


  "BaseController" should "Should return success status when code is OK " in {
    Assert.assertEquals(true, true);
    val controller = new BaseController(null, null)
    val result :Result = controller.result("OK","Success")
    Assert.assertEquals(result.header.status, 200)
  }

  "BaseController" should "Should return bad request status when code is CLIENT_ERROR " in {
    Assert.assertEquals(true, true);
    val controller = new BaseController(null, null)
    val result :Result = controller.result("CLIENT_ERROR","Error")
    Assert.assertEquals(result.header.status, 400)
  }

  "BaseController" should "Should return InternalServerError status when code is SERVER_ERROR " in {
    Assert.assertEquals(true, true);
    val controller = new BaseController(null, null)
    val result :Result = controller.result("SERVER_ERROR","Error")
    Assert.assertEquals(result.header.status, 500)
  }

  "BaseController" should "Should return RequestTimeout status when code is REQUEST_TIMEOUT " in {
    Assert.assertEquals(true, true);
    val controller = new BaseController(null, null)
    val result :Result = controller.result("REQUEST_TIMEOUT","Error")
    Assert.assertEquals(result.header.status, 408)
  }

  "BaseController" should "Should return NotFound status when code is RESOURCE_NOT_FOUND " in {
    Assert.assertEquals(true, true);
    val controller = new BaseController(null, null)
    val result :Result = controller.result("RESOURCE_NOT_FOUND","Error")
    Assert.assertEquals(result.header.status, 404)
  }

  "BaseController" should "Should return Forbidden status when code is FORBIDDEN " in {
    Assert.assertEquals(true, true);
    val controller = new BaseController(null, null)
    val result :Result = controller.result("FORBIDDEN","Forbidden")
    Assert.assertEquals(result.header.status, 403)
  }

}
