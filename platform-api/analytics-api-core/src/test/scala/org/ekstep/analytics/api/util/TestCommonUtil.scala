package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.BaseSpec
import org.joda.time.LocalDate
import java.io.File
import org.joda.time.DateTime
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeUtils
import org.ekstep.analytics.api.Range

class TestCommonUtil extends BaseSpec {

    "CommonUtil" should "test all utility methods" in {

        DateTimeUtils.setCurrentMillisFixed(1454650400000L);

        CommonUtil.roundDouble(12.7345, 2) should be(12.73);
        val resp1 = CommonUtil.errorResponse("com.test", "Test exception");
        resp1.params.err should be("SERVER_ERROR");
        resp1.params.errmsg should be("Test exception");
        resp1.id should be("com.test");
        val resp2 = CommonUtil.errorResponseSerialized("com.test", "Test exception");
        resp2 should include("Test exception");
        val resp3 = CommonUtil.OK("com.test", Map("ttl" -> 24.asInstanceOf[AnyRef]));
        resp3.params.err should be(null);
        resp3.params.status should be("successful");
        resp3.result.get should be(Map("ttl" -> 24.asInstanceOf[AnyRef]));
        CommonUtil.getDayRange(7) should be(Range(20160129, 20160205))
        CommonUtil.getMonthRange(2) should be(Range(201512, 201602));
        CommonUtil.getRemainingHours() should be(18);
        CommonUtil.getWeekRange(5) should be(Range(2015753, 2016705));
        CommonUtil.getWeeksBetween(1451650400000L, 1454650400000L) should be(5);
        
    }
}