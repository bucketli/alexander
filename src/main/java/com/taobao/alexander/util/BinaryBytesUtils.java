package com.taobao.alexander.util;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @date 2012-8-27下午04:38:50
 */
public class BinaryBytesUtils {
	public static Date getDate(byte[] bits, int length) {
		int year = 0;
		int month = 0;
		int day = 0;
		if (length != 0) {
			year = (bits[0] & 0xff) | ((bits[1] & 0xff) << 8);
			month = bits[2];
			day = bits[3];
		}
		if (length == 0 || ((year == 0) && (month == 0) && (day == 0))) {
			year = 1;
			month = 1;
			day = 1;
		}
		return DateTimeUtils.fastDateCreate(year, month, day);
	}

	public static Time getTime(byte[] bits, int length) {
		int hour = 0;
		int minute = 0;
		int seconds = 0;
		if (length != 0) {
			// bits[0] // skip tm->neg
			// binaryData.readLong(); // skip daysPart
			hour = bits[5];
			minute = bits[6];
			seconds = bits[7];
		}
		return DateTimeUtils.fastTimeCreate(hour, minute, seconds);
	}

	public static Timestamp getTimestamp(byte[] bits, int length) {
		int year = 0;
		int month = 0;
		int day = 0;
		int hour = 0;
		int minute = 0;
		int seconds = 0;
		int nanos = 0;
		if (length != 0) {
			year = (bits[0] & 0xff) | ((bits[1] & 0xff) << 8);
			month = bits[2];
			day = bits[3];
			if (length > 4) {
				hour = bits[4];
				minute = bits[5];
				seconds = bits[6];
			}
			if (length > 7) {
				// MySQL uses microseconds
				nanos = ((bits[7] & 0xff) | ((bits[8] & 0xff) << 8)
						| ((bits[9] & 0xff) << 16) | ((bits[10] & 0xff) << 24)) * 1000;
			}
		}
		if (length == 0 || ((year == 0) && (month == 0) && (day == 0))) {
			year = 1;
			month = 1;
			day = 1;
		}
		return DateTimeUtils.fastTimestampCreate(year, month, day, hour,
				minute, seconds, nanos);
	}

	public static double getDouble(byte[] bits) {
		long valueAsLong = (bits[0] & 0xff) | ((long) (bits[1] & 0xff) << 8)
				| ((long) (bits[2] & 0xff) << 16)
				| ((long) (bits[3] & 0xff) << 24)
				| ((long) (bits[4] & 0xff) << 32)
				| ((long) (bits[5] & 0xff) << 40)
				| ((long) (bits[6] & 0xff) << 48)
				| ((long) (bits[7] & 0xff) << 56);
		return Double.longBitsToDouble(valueAsLong);
	}

	public static float getFloat(byte[] bits) {
		int asInt = (bits[0] & 0xff) | ((bits[1] & 0xff) << 8)
				| ((bits[2] & 0xff) << 16) | ((bits[3] & 0xff) << 24);
		return Float.intBitsToFloat(asInt);
	}

	public static int getInt(byte[] bits) {
		int valueAsInt = (bits[0] & 0xff) | ((bits[1] & 0xff) << 8)
				| ((bits[2] & 0xff) << 16) | ((bits[3] & 0xff) << 24);
		return valueAsInt;
	}

	public static long getLong(byte[] bits) {
		long valueAsLong = (bits[0] & 0xff) | ((long) (bits[1] & 0xff) << 8)
				| ((long) (bits[2] & 0xff) << 16)
				| ((long) (bits[3] & 0xff) << 24)
				| ((long) (bits[4] & 0xff) << 32)
				| ((long) (bits[5] & 0xff) << 40)
				| ((long) (bits[6] & 0xff) << 48)
				| ((long) (bits[7] & 0xff) << 56);
		return valueAsLong;
	}

	public static short getShort(byte[] bits) {
		short asShort = (short) ((bits[0] & 0xff) | ((bits[1] & 0xff) << 8));
		return asShort;
	}

	public static byte getByte(byte[] bits) {
		byte asByte = (byte) ((bits[0] & 0xff));
		return asByte;
	}

	public static String getString(byte[] value, int length, String encoding) {
		String stringVal = null;
		if (encoding == null) {
			stringVal = new String(value);
		} else {
			try {
				stringVal = new String(value, 0, length, encoding);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
		return stringVal;
	}

	public static byte[] stringToBytes(String s, String encoding) {
		if (encoding == null) {
			return s.getBytes();
		} else {
			try {
				return s.getBytes(encoding);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static byte[] doubleToBytes(double d) {
		return longToBytes(Double.doubleToLongBits(d));
	}

	public static byte[] floatToBytes(float f) {
		return intToBytes(Float.floatToIntBits(f));
	}

	public static byte[] intToBytes(int i) {
		byte[] b = new byte[4];
		b[0] = (byte) (i & 0xff);
		b[1] = (byte) (i >>> 8);
		b[2] = (byte) (i >>> 16);
		b[3] = (byte) (i >>> 24);
		return b;
	}

	public static byte[] longToBytes(long l) {
		byte[] b = new byte[8];
		b[0] = (byte) (l & 0xff);
		b[1] = (byte) (l >>> 8);
		b[2] = (byte) (l >>> 16);
		b[3] = (byte) (l >>> 24);
		b[4] = (byte) (l >>> 32);
		b[5] = (byte) (l >>> 40);
		b[6] = (byte) (l >>> 48);
		b[7] = (byte) (l >>> 56);
		return b;
	}

	public static byte[] shortToBytes(short s) {
		byte[] b = new byte[2];
		b[0] = (byte) (s & 0xff);
		b[1] = (byte) (s >>> 8);
		return b;
	}

	public static byte[] byteToBytes(byte b) {
		return new byte[] { b };
	}

	public static byte[] timeToBytes(Time t) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(t);

		byte[] b = new byte[8];
		b[0] = (byte) 0;
		b[1] = (byte) (19 & 0xff);
		b[2] = (byte) (70 & 0xff);
		b[3] = (byte) (1 & 0xff);
		b[4] = (byte) (1 & 0xff);
		b[5] = (byte) (cal.get(Calendar.HOUR) & 0xff);
		b[6] = (byte) (cal.get(Calendar.MINUTE) & 0xff);
		b[7] = (byte) (cal.get(Calendar.SECOND) & 0xff);
		return b;
	}

	public static byte[] dateToBytes(Date d) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);

		byte[] b = new byte[5];
		b[0] = (byte) 0;
		b[1] = (byte) (cal.get(Calendar.YEAR) & 0xff);
		b[2] = (byte) (cal.get(Calendar.YEAR) >>> 8);
		b[3] = (byte) (cal.get(Calendar.MONTH) & 0xff);
		b[4] = (byte) (cal.get(Calendar.DATE) & 0xff);
		return b;
	}

	public static byte[] timeStampToBytes(Timestamp ts) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(ts);

		byte[] b = new byte[12];
		b[0] = (byte) 0;
		b[1] = (byte) (cal.get(Calendar.YEAR) & 0xff);
		b[2] = (byte) (cal.get(Calendar.YEAR) >>> 8);
		b[3] = (byte) (cal.get(Calendar.MONTH) & 0xff);
		b[4] = (byte) (cal.get(Calendar.DATE) & 0xff);
		b[5] = (byte) (cal.get(Calendar.HOUR) & 0xff);
		b[6] = (byte) (cal.get(Calendar.MINUTE) & 0xff);
		b[7] = (byte) (cal.get(Calendar.SECOND) & 0xff);
		b[8] = (byte) (ts.getNanos() & 0xff);
		b[9] = (byte) (ts.getNanos() >>> 8);
		b[10] = (byte) (ts.getNanos() >>> 16);
		b[11] = (byte) (ts.getNanos() >>> 24);
		return b;
	}
}
