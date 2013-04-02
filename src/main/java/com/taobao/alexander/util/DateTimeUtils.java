package com.taobao.alexander.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @date 2012-8-29下午01:09:48
 */
public class DateTimeUtils {
	public static Timestamp fastTimestampCreate(int year, int month, int day,
			int hour, int minute, int seconds, int secondsPart) {
		return fastTimestampCreate(null, year, month, day, hour, minute,
				seconds, secondsPart);
	}

	public static Timestamp fastTimestampCreate(TimeZone tz, int year,
			int month, int day, int hour, int minute, int seconds,
			int secondsPart) {
		Calendar cal = (tz == null) ? new GregorianCalendar()
				: new GregorianCalendar(tz);
		cal.clear();
		cal.set(year, month - 1, day, hour, minute, seconds);
		long tsAsMillis = 0;
		try {
			tsAsMillis = cal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			tsAsMillis = cal.getTime().getTime();
		}
		Timestamp ts = new Timestamp(tsAsMillis);
		ts.setNanos(secondsPart);
		return ts;
	}

	final static Date fastDateCreate(int year, int month, int day) {
		Calendar dateCal = new GregorianCalendar();
		dateCal.clear();
		dateCal.set(year, month - 1, day, 0, 0, 0);
		dateCal.set(Calendar.MILLISECOND, 0);
		long dateAsMillis = 0;
		try {
			dateAsMillis = dateCal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			dateAsMillis = dateCal.getTime().getTime();
		}
		return new Date(dateAsMillis);
	}

	final static Time fastTimeCreate(int hour, int minute, int second) {
		Calendar cal = new GregorianCalendar();
		cal.clear();
		// Set 'date' to epoch of Jan 1, 1970
		cal.set(1970, 0, 1, hour, minute, second);
		long timeAsMillis = 0;
		try {
			timeAsMillis = cal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			timeAsMillis = cal.getTime().getTime();
		}
		return new Time(timeAsMillis);
	}
}
