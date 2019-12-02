package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.{BaseSpec, Range, ResponseCode}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone, Duration}

class TestCommonUtil extends BaseSpec {

    "CommonUtil" should "test all utility methods" in {

        //DateTimeUtils.setCurrentMillisFixed(1454650400000L);

        val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30))
        val thisYear: Int = (new DateTime).getYear
        val thisMonth: Int = (new DateTime).getMonthOfYear
        val thisWeekNo = (new DateTime).getWeekOfWeekyear
        val monthYear = Integer.parseInt(s"$thisYear"+s"$thisMonth")

        val now = DateTime.now(DateTimeZone.UTC)
        val dateInt = Integer.parseInt(dateFormat.print(now).replace("-", ""))
        val remainingTime = new Duration(now, now.plusDays(1).withTimeAtStartOfDay).getStandardHours

        CommonUtil.roundDouble(12.7345, 2) should be(12.73);
        val resp1 = CommonUtil.errorResponse("com.test", "Test exception", ResponseCode.SERVER_ERROR.toString());
        resp1.params.err should be("SERVER_ERROR");
        resp1.params.errmsg should be("Test exception");
        resp1.id should be("com.test");
        val resp2 = CommonUtil.errorResponseSerialized("com.test", "Test exception", ResponseCode.SERVER_ERROR.toString());
        resp2 should include("Test exception");
        val resp3 = CommonUtil.OK("com.test", Map("ttl" -> 24.asInstanceOf[AnyRef]));
        resp3.params.err should be(null);
        resp3.params.status should be("successful");
        resp3.result.get should be(Map("ttl" -> 24.asInstanceOf[AnyRef]));
        CommonUtil.getDayRange(7) should be(Range(Integer.parseInt(dateFormat.print(now.minusDays(7)).replace("-", "")), dateInt))
        CommonUtil.getMonthRange(2) should be(Range(monthYear-2,  monthYear))
        CommonUtil.getRemainingHours() should be(remainingTime)
        CommonUtil.getWeeksBetween(1451650400000L, 1454650400000L) should be(5);
        
    }
}