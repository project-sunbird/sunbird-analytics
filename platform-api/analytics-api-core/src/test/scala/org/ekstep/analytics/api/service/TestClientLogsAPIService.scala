package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.JSONUtils


class TestClientLogsAPIService extends BaseSpec {
  val clientLogsAPIServiceMock: ClientLogsAPIService = mock[ClientLogsAPIService]
  val clientLogRequest: ClientLogRequest = mock[ClientLogRequest]

  "Client Log API Service" should "validate the request" in {
    // did is null
    val INVALIDREQUEST1 = "{\"request\":{\"context\":{\"pdata\":{\"id\":\"prod.diksha.portal\",\"ver\":\"1.0\",\"pid\":\"contentPlayer\"}},\"edata\":{\"dspec\":{\"os\":\"\",\"make\":\"\",\"mem\":0,\"idisk\":\"\",\"edisk\":\"\",\"scrn\":\"\",\"camera\":\"\",\"cpu\":\"\",\"sims\":0,\"uaspec\":{\"agent\":\"\",\"ver\":\"\",\"system\":\"\",\"platform\":\"\",\"raw\":\"\"}},\"crashts\":\"1560346371\",\"crash_logs\":\"asdasdasdasmdnflksandflkasnldfknaslkmdfn\"}}}"
    val requestObj1 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST1)

    requestObj1.validate.status should be(false)
    requestObj1.validate.msg should be("property: did is null or empty!")

    // empty request body
    val INVALIDREQUEST2 = "{\"request\":{}}"
    val requestObj2 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST2)

    requestObj2.validate.status should be(false)
    requestObj2.validate.msg should be("property: context is missing!")

    // empty context, without edata request body
    val INVALIDREQUEST3 = "{\"request\":{\"context\":{}}}"
    val requestObj3 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST3)

    requestObj3.validate.status should be(false)
    requestObj3.validate.msg should be("property: edata is missing!")

    // empty context, edata request body
    val INVALIDREQUEST4 = "{\"request\":{\"context\":{},\"edata\":{}}}"
    val requestObj4 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST4)

    requestObj4.validate.status should be(false)
    requestObj4.validate.msg should be("property: pdata is missing!")

    // empty context, empty pdata, without did request body
    val INVALIDREQUEST5 = "{\"request\":{\"context\":{\"pdata\":{}},\"edata\":{}}}"
    val requestObj5 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST5)

    requestObj5.validate.status should be(false)
    requestObj5.validate.msg should be("property: did is null or empty!")

    // context, with pdata.id, with did request body
    val INVALIDREQUEST6 = "{\"request\":{\"context\":{\"pdata\":{},\"did\":\"13123-13123-123123-1231231\"},\"edata\":{}}}"
    val requestObj6 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST6)

    requestObj6.validate.status should be(false)
    requestObj6.validate.msg should be("property: pdata.id is null or empty!")

    // context, with pdata.pid, with did request body
    val INVALIDREQUEST7 = "{\"request\":{\"context\":{\"pdata\":{\"id\":\"in.ekstep\"},\"did\":\"13123-13123-123123-1231231\"},\"edata\":{}}}"
    val requestObj7 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST7)

    requestObj7.validate.status should be(false)
    requestObj7.validate.msg should be("property: pdata.pid is null or empty!")

    // context, with pdata.ver, did request body
    val INVALIDREQUEST8 = "{\"request\":{\"context\":{\"pdata\":{\"id\":\"in.ekstep\",\"pid\":\"prod.diksha.app\"},\"did\":\"13123-13123-123123-1231231\"},\"edata\":{}}}"
    val requestObj8 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST8)

    requestObj8.validate.status should be(false)
    requestObj8.validate.msg should be("property: pdata.ver is null or empty!")

    // context, with pdata.id, did request body
    val INVALIDREQUEST9 = "{\"request\":{\"context\":{\"pdata\":{\"id\":\"in.ekstep\",\"pid\":\"prod.diksha.app\"},\"did\":\"13123-13123-123123-1231231\"},\"edata\":{}}}"
    val requestObj9 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST9)

    requestObj9.validate.status should be(false)
    requestObj9.validate.msg should be("property: pdata.ver is null or empty!")

    // context, with pdata.id, did, without edata.crashts request body
    val INVALIDREQUEST10 = "{\"request\":{\"context\":{\"pdata\":{\"id\":\"in.ekstep\",\"pid\":\"prod.diksha.app\",\"ver\":\"1.0\"},\"did\":\"13123-13123-123123-1231231\"},\"edata\":{\"crash_logs\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"}}}"
    val requestObj10 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST10)

    requestObj10.validate.status should be(false)
    requestObj10.validate.msg should be("property: crashts is null or empty!")

    // context, with pdata.id, did, without edata.crash_logs request body
    val INVALIDREQUEST11 = "{\"request\":{\"context\":{\"pdata\":{\"id\":\"in.ekstep\",\"pid\":\"prod.diksha.app\",\"ver\":\"1.0\"},\"did\":\"13123-13123-123123-1231231\"},\"edata\":{\"crashts\":\"1560346371\"}}}"
    val requestObj11 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST11)

    requestObj11.validate.status should be(false)
    requestObj11.validate.msg should be("property: crash_logs is null or empty!")
  }

  "request validation" should "pass validation for valid request" in {
    val VALIDREQUEST1 = "{\"request\":{\"context\":{\"pdata\":{\"id\":\"prod.diksha.portal\",\"ver\":\"1.0\",\"pid\":\"contentPlayer\"},\"did\":\"24124-3242-34-234234\"},\"edata\":{\"dspec\":{\"os\":\"\",\"make\":\"\",\"mem\":0,\"idisk\":\"\",\"edisk\":\"\",\"scrn\":\"\",\"camera\":\"\",\"cpu\":\"\",\"sims\":0,\"uaspec\":{\"agent\":\"\",\"ver\":\"\",\"system\":\"\",\"platform\":\"\",\"raw\":\"\"}},\"crashts\":\"1560346371\",\"crash_logs\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"}}}"
    val requestObj1 = JSONUtils.deserialize[ClientLogRequest](VALIDREQUEST1)

    requestObj1.validate.status should be(true)
    requestObj1.validate.msg should be("")
  }
}
