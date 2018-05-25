package com.golden.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * ���ڹ�����
 *
 */
public class DateUtils {
	public static Date stringToDate(String strDate, String pattern) throws ParseException {
		DateFormat df = new SimpleDateFormat(pattern);
		return df.parse(strDate);
	}

	public static Date stringToDate(String strDate)

	{
		DateFormat df = null;
		Date date = null;
		try {
			df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			date = df.parse(strDate);
		} catch (ParseException e) {
			df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			try {
				date = df.parse(strDate);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
		}
		return date;
	}

	public static String dateToString(Date date, String pattern) {
		DateFormat df = new SimpleDateFormat(pattern);
		return df.format(date);
	}

	public static String dateToString(Date date) {
		String pattern = "yyyy-MM-dd HH:mm:ss";
		DateFormat df = new SimpleDateFormat(pattern);
		return df.format(date);
	}

	public static int getGMTSecond(Date date) throws ParseException {
		return (int) (getGMTDateTime(date).getTime() / 1000L);
	}

	public static int getSecond(Date date) throws ParseException {
		return (int) (date.getTime() / 1000L);
	}

	public static Date getGMTDateTime(Date date) {
		DateFormat dateFormat = DateFormat.getDateInstance();
		int offset = dateFormat.getTimeZone().getOffset(date.getTime());
		date = new Date(date.getTime() - offset);
		return date;
	}

	public static short getMilSecond(Date datetime) throws ParseException {
		return (short) (int) (datetime.getTime() % 1000L);
	}

	public static int[] getGMTSecond(Date[] datetimes) throws ParseException {
		int count = datetimes.length;
		int[] datetime_temp = new int[count];
		for (int i = 0; i < count; i++) {
			datetime_temp[i] = ((int) (getGMTDateTime(datetimes[i]).getTime() / 1000L));
		}
		return datetime_temp;
	}

	public static int[] getSecond(Date[] datetimes) throws ParseException {
		int count = datetimes.length;
		int[] datetime_temp = new int[count];
		for (int i = 0; i < count; i++) {
			datetime_temp[i] = ((int) (datetimes[i].getTime() / 1000L));
		}
		return datetime_temp;
	}

	public static short[] getMilSecond(Date[] datetimes) throws ParseException {
		int count = datetimes.length;
		short[] datetime_temp = new short[count];
		for (int i = 0; i < count; i++) {
			datetime_temp[i] = ((short) (int) (datetimes[i].getTime() % 1000L));
		}
		return datetime_temp;
	}

	public static void getDate(Date[] datetimes, int[] datetimes_seconds, short[] mil_seconds) throws ParseException {
		long temp = 0L;
		int count = datetimes.length;
		for (int i = 0; i < count; i++) {
			temp = datetimes_seconds[i] * 1000L;
			datetimes[i] = new Date(temp + mil_seconds[i]);
		}
	}

	public static void getDate(String[] datetimes, int[] datetimes_seconds) throws ParseException {
		if (datetimes.length <= 0) {
			return;
		}
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int offset = format.getTimeZone().getOffset(new Date().getTime()) / 1000;
		Date dateConst = format.parse("1970-1-1 0:0:0");
		for (int i = 0; i < datetimes.length; i++) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(dateConst);
			calendar.add(13, datetimes_seconds[i] + offset);
			datetimes[i] = format.format(calendar.getTime());
		}
	}

	public static Date getDate(int datetime_second, short mil_second) {
		long temp = 0L;
		temp = datetime_second * 1000L;
		return new Date(temp + mil_second);
	}

	public static Date longToDate(long currentTime, String formatType) {
		Date dateOld = new Date(currentTime); // 根据long类型的毫秒数生命�?个date类型的时�?
		String sDateTime = dateToString(dateOld, formatType); // 把date类型的时间转换为string
		Date date = null;
		try {
			date = stringToDate(sDateTime, formatType);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // 把String类型转换为Date类型
		return date;
	}

	public static String longToString(long currentTime, String formatType) {
		Date date = null;
		date = longToDate(currentTime, formatType);
		String strTime = dateToString(date, formatType); // date类型转成String
		return strTime;
	}

	public static String TimeStampDate(int timestampstring) {
		Long timestamp = Long.valueOf(timestampstring * 1000L);
		String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp.longValue()));
		return date;
	}

	public static String TimeStampDates(long timestampstring) {
		String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestampstring));
		return date;
	}
}
