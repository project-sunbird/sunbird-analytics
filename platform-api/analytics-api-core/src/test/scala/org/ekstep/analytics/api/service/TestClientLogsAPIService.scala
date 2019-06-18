package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.JSONUtils


class TestClientLogsAPIService extends BaseSpec {
  val clientLogsAPIServiceMock: ClientLogsAPIService = mock[ClientLogsAPIService]
  val clientLogRequest: ClientLogRequest = mock[ClientLogRequest]

  "Client Log API Service" should "validate the request" in {
    // did is null
    val INVALIDREQUEST1 = "{\"request\":{\"pdata\":{\"id\":\"contentPlayer\",\"ver\":\"1.0\",\"pid\":\"prod.diksha.portal\"},\"context\":{\"dspec\":{\"os\":\"mac\",\"make\":\"\",\"mem\":0,\"idisk\":\"\",\"edisk\":\"\",\"scrn\":\"\",\"camera\":\"\",\"cpu\":\"\",\"sims\":0,\"uaspec\":{\"agent\":\"\",\"ver\":\"\",\"system\":\"\",\"platform\":\"\",\"raw\":\"\"}}},\"logs\":[{\"id\":\"13123-123123-12312-3123\",\"ts\":1560346371,\"log\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"},{\"id\":\"13123-123123-12312-3123\",\"ts\":1560346371,\"log\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"}]}}"
    val requestObj1 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST1)

    requestObj1.validate.status should be(false)
    requestObj1.validate.msg should be("property: context.did is null or empty!")

    // empty request body
    val INVALIDREQUEST2 = "{\"request\":{}}"
    val requestObj2 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST2)

    requestObj2.validate.status should be(false)
    requestObj2.validate.msg should be("property: context is missing!")

    // empty context, without edata request body
    val INVALIDREQUEST3 = "{\"request\":{\"context\":{}}}"
    val requestObj3 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST3)

    requestObj3.validate.status should be(false)
    requestObj3.validate.msg should be("property: pdata is missing!")

    // empty context, edata request body
    val INVALIDREQUEST4 = "{\"request\":{\"context\":{},\"pdata\":{}}}"
    val requestObj4 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST4)

    requestObj4.validate.status should be(false)
    requestObj4.validate.msg should be("property: logs is missing!")

    // context, with pdata.id, with did request body
    val INVALIDREQUEST6 = "{\"request\":{\"context\":{\"did\":\"13123-13123-123123-1231231\"},\"pdata\":{}, \"logs\":[]}}"
    val requestObj6 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST6)

    requestObj6.validate.status should be(false)
    requestObj6.validate.msg should be("property: pdata.id is null or empty!")

    // context, with pdata.pid, with did request body
    val INVALIDREQUEST7 = "{\"request\":{\"context\":{\"did\":\"13123-13123-123123-1231231\"},\"pdata\":{\"id\":\"in.ekstep\"}, \"logs\":[]}}"
    val requestObj7 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST7)

    requestObj7.validate.status should be(false)
    requestObj7.validate.msg should be("property: pdata.pid is null or empty!")

    // context, with pdata.ver, did request body
    val INVALIDREQUEST8 = "{\"request\":{\"context\":{\"did\":\"13123-13123-123123-1231231\"}, \"pdata\":{\"id\":\"in.ekstep\",\"pid\":\"prod.diksha.app\"}, \"logs\":[]}}"
    val requestObj8 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST8)

    requestObj8.validate.status should be(false)
    requestObj8.validate.msg should be("property: pdata.ver is null or empty!")


    // context, with pdata.id, did, without logs request body
    val INVALIDREQUEST11 = "{\"request\":{\"context\":{\"did\":\"13123-13123-123123-1231231\"},\"pdata\":{\"id\":\"in.ekstep\",\"pid\":\"prod.diksha.app\",\"ver\":\"1.0\"}}}"
    val requestObj11 = JSONUtils.deserialize[ClientLogRequest](INVALIDREQUEST11)

    requestObj11.validate.status should be(false)
    requestObj11.validate.msg should be("property: logs is missing!")
  }

  "request validation" should "pass validation for valid request" in {
    val VALIDREQUEST1 = "{\"request\":{\"pdata\":{\"id\":\"contentPlayer\",\"ver\":\"1.0\",\"pid\":\"prod.diksha.portal\"},\"context\":{\"did\":\"1242-234234-24234-234234\",\"dspec\":{\"os\":\"mac\",\"make\":\"\",\"mem\":0,\"idisk\":\"\",\"edisk\":\"\",\"scrn\":\"\",\"camera\":\"\",\"cpu\":\"\",\"sims\":0,\"uaspec\":{\"agent\":\"\",\"ver\":\"\",\"system\":\"\",\"platform\":\"\",\"raw\":\"\"}},\"extras\":{\"key-123\":\"value-123\",\"key-1234\":\"value-123\",\"key-1235\":\"value-123\"}},\"logs\":[{\"id\":\"13123-123123-12312-3123\",\"ts\":1560346371,\"log\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"},{\"id\":\"13123-123123-12312-3123\",\"ts\":1560346371,\"log\":\"Exception in thread \\\"main\\\" java.lang.NullPointerException\\n        at com.example.myproject.Book.getTitle(Book.java:16)\\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\\n\"}]}}"
    val requestObj1 = JSONUtils.deserialize[ClientLogRequest](VALIDREQUEST1)

    requestObj1.validate.status should be(true)
    requestObj1.validate.msg should be("")
  }
}
