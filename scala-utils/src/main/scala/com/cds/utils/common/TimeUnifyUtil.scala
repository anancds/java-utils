package com.cds.utils.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

import scala.collection.mutable

/**
  * Created by chendongsheng5 on 2017/3/29.
  */
object TimeUnifyUtil {
  val YEAR = "Y"
  val MONTH = "M"
  val DAY = "D"
  val HOUR = "H"
  val MINUTE = "F"
  val SECOND = "S"

  val TIMEZONE: String = TimeZone.getDefault.getID

  /**
    * 获取某年第一天日期
    *
    * @param year     年（如2016）
    * @param timeZone 时区
    * @return 该年第一天
    */
  def getYearFirst(year: Int, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.clear()
    calendar.set(Calendar.YEAR, year)
    calendar.getTime
  }

  /**
    * 获取某年最后一天日期
    *
    * @param year     年（如2016）
    * @param timeZone 时区
    * @return 该年最后一天
    */
  def getYearLast(year: Int, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.clear()
    calendar.set(Calendar.YEAR, year)
    calendar.roll(Calendar.DAY_OF_YEAR, -1)
    calendar.getTime
  }

  /**
    * 获取某年第一天时间
    *
    * @param year     年（如2016）
    * @param timeZone 时区
    * @return 返回该年第一天时间
    */
  def getTimeYearFirst(year: Int, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.clear()
    calendar.set(Calendar.YEAR, year)
    calendar.getTime
  }

  /**
    * 获取某年最后一天时间
    *
    * @param year     年（如2016）
    * @param timeZone 时区
    * @return 返回该年最后一天时间
    */
  def getTimeYearLast(year: Int, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.clear()
    calendar.set(Calendar.YEAR, year)
    calendar.add(Calendar.YEAR, 1)
    calendar.add(Calendar.MILLISECOND, -1)
    calendar.getTime
  }

  /**
    * 获取某月的第一天
    *
    * @param month       月（如2016-12）
    * @param monthFormat 月格式（如yyyy-MM）
    * @param timeZone    时区
    * @return 返回该月第一天
    */
  def getMonthFirst(month: String, monthFormat: String, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(month, monthFormat, timeZone))
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.getTime
  }

  /**
    * 获取某月的最后一天
    *
    * @param month       月（如2016-12）
    * @param monthFormat 月格式（如yyyy-MM）
    * @param timeZone    时区
    * @return 返回该月最后一天
    */
  def getMonthLast(month: String, monthFormat: String, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(month, monthFormat, timeZone))
    calendar.add(Calendar.MONTH, 1)
    calendar.set(Calendar.DAY_OF_MONTH, 0)
    calendar.getTime
  }

  /**
    * 时间戳获取某月的第一天
    *
    * @param month       月(2016-12)
    * @param monthFormat 月格式(yyyy-MM)
    * @param timeZone    时区
    * @return 返回该月第一天时间
    */
  def getTimeMonthFirst(month: String, monthFormat: String, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(month, monthFormat, timeZone))
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.getTime
  }

  /**
    * 时间戳获取某月的最后一天
    *
    * @param month       月(2016-12)
    * @param monthFormat 月格式(yyyy-MM)
    * @param timeZone    时区
    * @return 返回该月最后一天时间
    */
  def getTimeMonthLast(month: String, monthFormat: String, timeZone: String): Date = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(month, monthFormat, timeZone))
    calendar.add(Calendar.MONTH, 1)
    calendar.add(Calendar.MILLISECOND, -1)
    calendar.getTime
  }

  /**
    * 获取一天开始的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时间时区
    * @return 该天开始时间字符串
    */
  def getTimeDayFirst(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取一天结束的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时间时区
    * @return 该天结束的时间字符串
    */
  def getTimeDayEnd(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.add(Calendar.DATE, 1)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.add(Calendar.MILLISECOND, -1)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取当前小时开始的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时间时区
    * @return 当前小时开始的时间字符串
    */
  def getTimeHourFirst(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取当前小时结束的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时间时区
    * @return 当前小时结束的时间字符串
    */
  def getTimeHourEnd(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.add(Calendar.HOUR_OF_DAY, 1)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.add(Calendar.MILLISECOND, -1)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取当前分钟开始的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 当前分钟开始的时间戳
    */
  def getTimeMinuteFirst(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取当前分钟结束的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 当前分钟结束的时间字符串
    */
  def getTimeMinuteEnd(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.add(Calendar.MINUTE, 1)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.add(Calendar.MILLISECOND, -1)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取当前秒开始的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 当前秒开始的时间字符串
    */
  def getTimeSecondFirst(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.set(Calendar.MILLISECOND, 0)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取当前秒结束的时间戳
    *
    * @param date     时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 当前秒结束的时间字符串
    */
  def getTimeSecondEnd(date: String, format: String, timeZone: String): String = {
    val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(parseToDate(date, format, timeZone))
    calendar.add(Calendar.SECOND, 1)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.add(Calendar.MILLISECOND, -1)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 将时间字符串转化为时间
    *
    * @param value     时间字符串
    * @param formatStr 时间格式
    * @param timeZone  时间时区
    * @return 转化后的时间
    */
  def parseToDate(value: String, formatStr: String, timeZone: String): Date = {
    val format = new SimpleDateFormat(formatStr)
    format.setTimeZone(TimeZone.getTimeZone(timeZone))
    format.parse(value)
  }
  def parseToSqlDate(value:String,formatStr:String,timeZone:String):java.sql.Date={
    val date1=parseToDate(value,formatStr,timeZone)
    val sqlDate:java.sql.Date=new java.sql.Date(date1.getTime)
    sqlDate
  }

  /**
    * 将时间字符串转化为long
    *
    * @param value     时间字符串
    * @param formatStr 时间格式
    * @param timeZone  时间时区
    * @return 转化后的Long
    */
  def parseDateToLong(value: String, formatStr: String, timeZone: String): Long = {
    val format = new SimpleDateFormat(formatStr)
    format.setTimeZone(TimeZone.getTimeZone(timeZone))
    format.parse(value).getTime
  }

  /**
    * 将时间转化为字符串
    *
    * @param date      时间
    * @param formatStr 时间格式
    * @param timeZone  时间时区
    * @return 转化后时间字符串
    */
  def formatDate(date: Date, formatStr: String, timeZone: String): String = {
    val format = new SimpleDateFormat(formatStr)
    format.setTimeZone(TimeZone.getTimeZone(timeZone))
    format.format(date)
  }

  /**
    * 将Long转化为时间字符串
    *
    * @param time      long时间
    * @param formatStr 时间格式
    * @param timeZone  时间时区
    * @return 转化后的时间字符串
    */
  def formatLongDate(time: Long, formatStr: String, timeZone: String): String = {
    val date = new Date(time)
    val format = new SimpleDateFormat(formatStr)
    format.setTimeZone(TimeZone.getTimeZone(timeZone))
    format.format(date)
  }

  /**
    * 根据需要获取的时间级别格式化时间(时间字符串转化为时间字符串)
    *
    * @param dateStr     时间
    * @param valueFormat 格式化后格式
    * @param format      原始格式
    * @param timeZone    时区
    * @return 格式化后的时间字符串
    */
  def getDateWithLevel(dateStr: String, valueFormat: String, format: String, timeZone: String): String = {
    val date = parseToDate(dateStr, format, timeZone)
    formatDate(date, valueFormat, timeZone)
  }

  /**
    * 根据需要获取的时间级别格式化时间戳
    *
    * @param timeLong    long时间
    * @param valueFormat 格式化后格式
    * @param timeZone    时区
    * @return 格式化后的时间字符串
    */
  def getLongTimeWithLevel(timeLong: Long, valueFormat: String, timeZone: String): String = {
    val date = new Date(timeLong)
    formatDate(date, valueFormat, timeZone)
  }

  @SuppressWarnings(Array("use getDateWithLevel instead"))
  def formatDay(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    formatDate(time, "yyyy-MM-dd", timeZone)
  }

  /**
    * 针对时间字符串加减天数
    *
    * @param dateStr  时间字符串
    * @param value    加减的天数
    * @param timeZone 时区
    * @param format   时间格式
    * @return 返回转化后的时间字符串
    */
  def dateStrAddSub(dateStr: String, value: Int, timeZone: String, format: String): String = {
    val date = parseToDate(dateStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(date)
    calendar.add(Calendar.DATE, value)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 针对时间加减天数
    *
    * @param dateStr  时间字符串
    * @param value    加减天数
    * @param timeZone 时区
    * @param format   时间格式
    * @return 转化后的时间字符串
    */
  def timeStrAddSubMinSecond(dateStr: String, value: Int, timeZone: String, format: String): String = {
    val date = parseToDate(dateStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(date)
    calendar.add(Calendar.MILLISECOND, value)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 针对时间加减秒
    *
    * @return
    */
  def timeStrAddSubMinSecond(timeLong: Long, value: Int, timeZone: String, format: String): String = {
    val time = new Date(timeLong)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    calendar.add(Calendar.MILLISECOND, value)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取下一天的时间戳
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 下一天时间字符串
    */
  def getNextTotalDay(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    calendar.add(Calendar.DATE, 1)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取前一天的时间戳
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 前一天的时间字符串
    */
  def getLastTotalDay(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.add(Calendar.MILLISECOND, -1)
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取整点小时，当前小时有分钟和秒不为0时，增加为下一小时
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 获取整点小时字符串
    */
  def getHourlyHour(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    if (calendar.get(Calendar.MINUTE) != 0 || calendar.get(Calendar.SECOND) != 0 || calendar.get(Calendar.MILLISECOND) != 0) {
      calendar.add(Calendar.HOUR_OF_DAY, 1)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
    }
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取整点小时，当前小时有分钟和秒不为0时，减少为上一小时
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 获取整点小时
    */
  def getHourlyLastHour(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    if (calendar.get(Calendar.MINUTE) != 59 || calendar.get(Calendar.SECOND) != 59 || calendar.get(Calendar.MILLISECOND) != 999) {
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      calendar.add(Calendar.MILLISECOND, -1)
    }
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取整点分钟，当前秒不为0时，增加为下一分钟
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 获取整点分钟
    */
  def getMinutelyMinute(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    if (calendar.get(Calendar.SECOND) == 0 && calendar.get(Calendar.MILLISECOND) == 0) {
      calendar.getTime
    } else {
      calendar.add(Calendar.MINUTE, 1)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
    }
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取整点分钟，当前秒不为0时，增加为下一分钟
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 获取整点分钟
    */
  def getMinutelyLastMinute(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    if (calendar.get(Calendar.SECOND) == 59 && calendar.get(Calendar.MILLISECOND) == 999) {
      calendar.getTime
    } else {
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      calendar.add(Calendar.MILLISECOND, -1)
    }
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取整点秒，当前毫秒不为0时，增加为下一秒
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 获取整点秒
    */
  def getSecondlySecond(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    if (calendar.get(Calendar.MILLISECOND) == 0) {
      calendar.getTime
    } else {
      calendar.add(Calendar.SECOND, 1)
      calendar.set(Calendar.MILLISECOND, 0)
    }
    formatDate(calendar.getTime, format, timeZone)
  }

  /**
    * 获取整点分钟，当前秒不为0时，增加为下一分钟
    *
    * @param timeStr  时间
    * @param format   时间格式
    * @param timeZone 时区
    * @return 获取整点秒
    */
  def getSecondlyLastSecond(timeStr: String, format: String, timeZone: String): String = {
    val time = parseToDate(timeStr, format, timeZone)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calendar.setTime(time)
    if (calendar.get(Calendar.MILLISECOND) == 999) {
      calendar.getTime
    } else {
      calendar.set(Calendar.MILLISECOND, 0)
      calendar.add(Calendar.MILLISECOND, -1)
    }
    formatDate(calendar.getTime, format, timeZone)
  }


  /**
    * 根据时间格式 生成年月日时分秒的格式
    *
    * @param format
    * @return
    */
  def generateTimeFormatSeq(format: String): Seq[String] = {
    format match {
      case "yyyy-MM-dd" => Seq("yyyy", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd HH", "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS")
      case "yyyy-MM-dd HH:mm:ss.SSS" => Seq("yyyy", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd HH", "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS")
      case other => sys.error(format + " not support!")
    }
  }

  /**
    * 补全日期时间字符串到时间戳，处理时间戳查询
    *
    * @param dateStrs 日期序列
    * @return 补全后的字符串序列
    */
  def convertDateToTimeStamp(dateStrs: Seq[String]): Seq[String] = {
    dateStrs.map(t => {
      t + " 00:00:00.000"
    })
  }

  /**
    * 补全日期字符串到时间戳天结束
    *
    * @param dateStrs 日期序列
    * @return 补全后的时间字符串序列
    */
  def convertDateToTimeStampEnd(dateStrs: Seq[String]): Seq[String] = {
    dateStrs.map(t => {
      t + " 23:59:59.999"
    })
  }

  /**
    * 比较两个时间的大小
    *
    * @param date1    时间1
    * @param date2    时间2
    * @param format   时间格式
    * @param timeZone 时间时区
    * @return 返回1 代表时间1大
    *         返回0 代表时间相等
    *         返回-1 代表时间1小
    */
  def compareDate(date1: String, date2: String, format: String, timeZone: String): Int = {
    val d1 = parseToDate(date1, format, timeZone)
    val d2 = parseToDate(date2, format, timeZone)
    if (d1.getTime - d2.getTime > 0) 1
    else if (d1.getTime - d2.getTime == 0) 0
    else -1
  }

  /**
    * 日期方法，该方法为通用方法，目的是根据起始和结束日期得到指定日期层级（Y, M, D）下的所有日期。
    * 例如：start: 2015-02-23 end: 2016-04-15 level: Y 输出：2015, 2016
    *
    * @param start 起始日期
    * @param end   结束日期
    * @return 有序的日期序列
    */
  def getDate(start: String, end: String, valueFormat: String, format: String, timeZone: String): Seq[String] = {

    //保证按照天来循环，这样得到的year才是正确的
    val result = new mutable.TreeSet[String]()
    val calBegin: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    calBegin.setTime(parseToDate(start, format, timeZone))
    val endDay = parseToDate(end, format, timeZone)
    while (endDay.after(calBegin.getTime)) {
      result += formatDate(calBegin.getTime, valueFormat, timeZone)
      calBegin.add(Calendar.DAY_OF_YEAR, 1)
      calBegin.set(Calendar.HOUR_OF_DAY, 23)
      calBegin.set(Calendar.MINUTE, 59)
      calBegin.set(Calendar.SECOND, 59)
      calBegin.set(Calendar.MILLISECOND, 999)
    }
    //补上跳出循环的日期
    result += formatDate(endDay, valueFormat, timeZone)
    result.toSeq
  }

  /**
    * 获取时间范围内的小时
    *
    * @param start       开始时间
    * @param end         结束时间
    * @param valueFormat 返回的格式
    * @param format      传入的时间格式
    * @param timeZone    时区
    * @return 时间范围内的精确到小时时间
    */
  def getDateHour(start: String, end: String, valueFormat: String, format: String, timeZone: String): Seq[String] = {

    val result = new mutable.TreeSet[String]()
    val calBegin: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))

    calBegin.setTime(parseToDate(start, format, timeZone))
    val endDay = parseToDate(end, format, timeZone)
    while (endDay.after(calBegin.getTime)) {
      result += formatDate(calBegin.getTime, valueFormat, timeZone)
      calBegin.add(Calendar.HOUR_OF_DAY, 1)
      calBegin.set(Calendar.MINUTE, 59)
      calBegin.set(Calendar.SECOND, 59)
      calBegin.set(Calendar.MILLISECOND, 999)
    }
    //补上跳出循环的日期
    result += formatDate(endDay, valueFormat, timeZone)
    result.toSeq
  }

  /**
    * 获取时间范围内的分钟
    *
    * @param start       开始时间
    * @param end         结束时间
    * @param valueFormat 返回的格式
    * @param format      传入的时间格式
    * @param timeZone    时区
    * @return 时间范围内的精确到分钟时间
    */
  def getDateMinute(start: String, end: String, valueFormat: String, format: String, timeZone: String): Seq[String] = {

    val result = new mutable.TreeSet[String]()
    val calBegin: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))

    calBegin.setTime(parseToDate(start, format, timeZone))
    val endDay = parseToDate(end, format, timeZone)
    while (endDay.after(calBegin.getTime)) {
      result += formatDate(calBegin.getTime, valueFormat, timeZone)
      calBegin.add(Calendar.MINUTE, 1)
      calBegin.set(Calendar.SECOND, 59)
      calBegin.set(Calendar.MILLISECOND, 999)
    }
    //补上跳出循环的日期
    result += formatDate(endDay, valueFormat, timeZone)
    result.toSeq
  }

  /**
    * 获取时间范围内的秒
    *
    * @param start       开始时间
    * @param end         结束时间
    * @param valueFormat 返回的格式
    * @param format      传入的时间格式
    * @param timeZone    时区
    * @return 时间范围内的精确到秒时间
    */
  def getDateSecond(start: String, end: String, valueFormat: String, format: String, timeZone: String): Seq[String] = {

    val result = new mutable.TreeSet[String]()
    val calBegin: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))

    calBegin.setTime(parseToDate(start, format, timeZone))
    val endDay = parseToDate(end, format, timeZone)
    while (endDay.after(calBegin.getTime)) {
      result += formatDate(calBegin.getTime, valueFormat, timeZone)
      calBegin.add(Calendar.SECOND, 1)
      calBegin.set(Calendar.MILLISECOND, 999)
    }
    //补上跳出循环的日期
    result += formatDate(endDay, valueFormat, timeZone)
    result.toSeq
  }

  def main(args: Array[String]) {

    val dateStr = "2016-12-01 10:55:59.555"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("GMT"))
    println(format.parse(dateStr).getTime)
    println(format.format(format.parse(dateStr)))

    val format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format1.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    println(format1.parse(dateStr).getTime)
    println(format1.format(format1.parse(dateStr)))

  }
}
