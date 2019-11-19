package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.{EventId, Response}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestBaseAdapter extends SparkSpec {

    "BaseAdapter" should "pass all test cases" in {
        val respStr = "{\"id\":\"analytics.device-profile\",\"ver\":\"1.0\",\"ts\":\"2019-11-12T07:28:10.555+00:00\",\"params\":{\"resmsgid\":\"e7f845ae-88f9-40b5-9cf3-1b2efc722879\",\"status\":\"successful\",\"client_key\":null},\"responseCode\":\"OK\",\"result\":{\"userDeclaredLocation\":{\"state\":\"Karnataka\",\"district\":\"KOPPAL\"},\"ipLocation\":{\"state\":\"Karnataka\",\"district\":\"BENGALURU URBAN SOUTH\"}}}"
        val response = JSONUtils.deserialize[Response](respStr)
        ContentAdapter.checkResponse(response)

    }

    it should "throw DataAdapterException" in {
        val respStr = "{\"id\":\"analytics.device-profile\",\"ver\":\"1.0\",\"ts\":\"2019-11-12T07:28:10.555+00:00\",\"params\":{\"resmsgid\":\"e7f845ae-88f9-40b5-9cf3-1b2efc722879\",\"status\":\"Failed\",\"client_key\":null},\"responseCode\":\"400 Not Found\",\"result\":{\"userDeclaredLocation\":{\"state\":\"Karnataka\",\"district\":\"KOPPAL\"},\"ipLocation\":{\"state\":\"Karnataka\",\"district\":\"BENGALURU URBAN SOUTH\"}}}"
        val response = JSONUtils.deserialize[Response](respStr)

        the[DataAdapterException] thrownBy {
            ContentAdapter.checkResponse(response)
        }

    }

}