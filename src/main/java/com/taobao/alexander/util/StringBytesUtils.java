package com.taobao.alexander.util;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @date 2012-8-27下午04:38:50
 */
public class StringBytesUtils {
	public static Date getDate(byte[] bits, int length, String encoding) {
		String dateStr = getString(bits, length, encoding);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
		try {
			java.util.Date date = sdf.parse(dateStr);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			return DateTimeUtils.fastDateCreate(cal.get(Calendar.YEAR),
					cal.get(Calendar.MONTH), cal.get(Calendar.DATE));
		} catch (ParseException e) {
			throw new RuntimeException(
					"date string parse error!date string is " + dateStr, e);
		}
	}

	public static Time getTime(byte[] bits, int length, String encoding) {
		String dateStr = getString(bits, length, encoding);
		SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
		try {
			java.util.Date date = sdf.parse(dateStr);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			return DateTimeUtils.fastTimeCreate(cal.get(Calendar.HOUR),
					cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
		} catch (ParseException e) {
			throw new RuntimeException(
					"date string parse error!date string is " + dateStr, e);
		}
	}

	public static Timestamp getTimestamp(byte[] bits, int length,
			String encoding) {
		String dateStr = getString(bits, length, encoding);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		try {
			java.util.Date date = sdf.parse(dateStr);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			return DateTimeUtils.fastTimestampCreate(cal.get(Calendar.YEAR),
					cal.get(Calendar.MONTH), cal.get(Calendar.DATE),
					cal.get(Calendar.HOUR), cal.get(Calendar.MINUTE),
					cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
		} catch (ParseException e) {
			throw new RuntimeException(
					"date string parse error!date string is " + dateStr, e);
		}
	}

	public static double getDouble(byte[] bits,String encoding) {
		String numberStr = getString(bits, bits.length, encoding);
		return Double.valueOf(numberStr);
	}

	public static float getFloat(byte[] bits,String encoding) {
		String numberStr = getString(bits, bits.length, encoding);
		return Float.valueOf(numberStr);
	}

	public static int getInt(byte[] bits,String encoding) {
		String numberStr = getString(bits, bits.length, encoding);
		return Integer.valueOf(numberStr);
	}

	public static long getLong(byte[] bits,String encoding) {
		String numberStr = getString(bits, bits.length, encoding);
		return Long.valueOf(numberStr);
	}

	public static short getShort(byte[] bits,String encoding) {
		String numberStr = getString(bits, bits.length, encoding);
		return Short.valueOf(numberStr);
	}

	public static byte getByte(byte[] bits,String encoding) {
		String numberStr = getString(bits, bits.length, encoding);
		return Byte.valueOf(numberStr);
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

	public static byte[] doubleToBytes(double d,String encoding) {
	    return stringToBytes(String.valueOf(d),encoding);
	}

	public static byte[] floatToBytes(float f,String encoding) {
	    return stringToBytes(String.valueOf(f),encoding);
	}

	public static byte[] intToBytes(int i,String encoding) {
	    return stringToBytes(String.valueOf(i),encoding);
	}

	public static byte[] longToBytes(long l,String encoding) {
	    return stringToBytes(String.valueOf(l),encoding);
	}

	public static byte[] shortToBytes(short s,String encoding) {
	    return stringToBytes(String.valueOf(s),encoding);
	}

	public static byte[] byteToBytes(byte b,String encoding) {
		return stringToBytes(String.valueOf(b), encoding);
	}

	public static byte[] timeToBytes(Time t,String encoding) {
		SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
		return stringToBytes(sdf.format(t),encoding);
	}

	public static byte[] dateToBytes(Date d,String encoding) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
		return stringToBytes(sdf.format(d),encoding);
	}

	public static byte[] timeStampToBytes(Timestamp ts,String encoding) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return stringToBytes(sdf.format(ts),encoding);
	}
}
