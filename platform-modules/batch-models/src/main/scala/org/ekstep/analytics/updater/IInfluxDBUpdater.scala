package org.ekstep.analytics.updater

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.commons.lang3.StringUtils
import org.joda.time.DateTimeZone

/**
 * @author Mahesh Kumar Gangula
 */

trait IInfluxDBUpdater {
	
    val dayPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    val weekPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-ww").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    val monthPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
	
	def getDateTime(periodVal: Int): (DateTime, String) = {
//		Integer.toString(periodVal)
		val period = periodVal.toString();
		period.size match {
			case 8 => (dayPeriod.parseDateTime(period).withTimeAtStartOfDay(), "day");
			case 7 =>
				val week = period.substring(0, 4) + "-" + period.substring(5, period.length);
				val firstDay = weekPeriod.parseDateTime(week)
				val lastDay = firstDay.plusDays(6);
				(lastDay.withTimeAtStartOfDay(), "week");
			case 6 => (monthPeriod.parseDateTime(period).withTimeAtStartOfDay(), "month");
		}
	}
}