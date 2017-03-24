package com.cds.utils.common

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by chendongsheng5 on 2017/3/24.
  */
object TimeUtil {

  val LEVELS = List(TimeUnifyUtil.YEAR, TimeUnifyUtil.MONTH, TimeUnifyUtil.DAY, TimeUnifyUtil.HOUR, TimeUnifyUtil.MINUTE, TimeUnifyUtil.SECOND)
  val timeLevels = Seq(TimeUnifyUtil.HOUR, TimeUnifyUtil.MINUTE, TimeUnifyUtil.SECOND)
  /**
    * 对给定的时间范围来按照年划分片段，例如："2015-01-03"到"2017-06-06"
    * 划分的片段为：(2015-01-03, 2015-12-31), (2016-01-01, 2016-12-31), (2017-01-01, 2017-06-06)
    *
    * @param start 起始时间 yyyy-MM-dd
    * @param end 终止时间 yyyy-MM-dd
    * @return 年片段序列
    */
  def getYearSegment(start: String,
                     end: String,
                     yearFormat: String,
                     format: String,
                     timeZone: String): Seq[(String, String)] = {
    val yearList = getYear(start, end, yearFormat, format, timeZone)
    val result = yearList.size match {
      case 0 => sys.error(" Can not get date information! ")
      case 1 => Seq(Tuple2(start, end))
      case _ => yearSegment(start, end, yearList, format, timeZone)
    }
    result
  }

  /**
    * 对给定的时间范围来按照年划分片段，例如："2015-01-03"到"2017-06-06"
    * 划分的片段为：(2015-01-03, 2015-12-31), (2016-01-01, 2016-12-31), (2017-01-01, 2017-06-06)
    *
    * @param start 起始时间 yyyy-MM-dd
    * @param end 终止时间 yyyy-MM-dd
    * @return 年片段序列
    */
  def getTimeYearSegment(start: String,
                         end: String,
                         yearFormat: String,
                         format: String,
                         timeZone: String): Seq[(String, String)] = {
    val yearList = getYear(start, end, yearFormat, format, timeZone)
    val result = yearList.size match {
      case 0 => sys.error(" Can not get date information! ")
      case 1 => Seq(Tuple2(start, end))
      case _ => yearTimeSegment(start, end, yearList, format, timeZone)
    }
    result
  }

  /**
    * 根据年列表来得到每年的起始和结束日期
    *
    * @param start 起始时间 yyyy-MM-dd
    * @param end 终止时间 yyyy-MM-dd
    * @param yearList 年序列
    * @return 年片段序列
    */
  def yearSegment(start: String,
                  end: String,
                  yearList: Seq[String],
                  format: String,
                  timeZone: String): Seq[(String, String)] = {
    val result = new ListBuffer[(String, String)]
    result += Tuple2(start, TimeUnifyUtil.formatDate(TimeUnifyUtil.getYearLast(yearList(0).toInt, timeZone), format, timeZone))
    //第二个元素到倒数第二个元素
    for (i <- 1 to yearList.size - 2) {
      result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getYearFirst(yearList(i).toInt, timeZone), format, timeZone),
        TimeUnifyUtil.formatDate(TimeUnifyUtil.getYearLast(yearList(i).toInt, timeZone), format, timeZone))
    }
    result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getYearFirst(yearList(yearList.size - 1).toInt, timeZone), format, timeZone), end)
    result
  }

  /**
    * 根据年列表来得到每年的起始和结束日期
    *
    * @param start 起始时间 yyyy-MM-dd
    * @param end 终止时间 yyyy-MM-dd
    * @param yearList 年序列
    * @return 年片段序列
    */
  def yearTimeSegment(start: String,
                      end: String,
                      yearList: Seq[String],
                      format: String,
                      timeZone: String): Seq[(String, String)] = {
    val result = new ListBuffer[(String, String)]
    result += Tuple2(start, TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeYearLast(yearList(0).toInt, timeZone), format, timeZone))
    //第二个元素到倒数第二个元素
    for (i <- 1 to yearList.size - 2) {
      result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeYearFirst(yearList(i).toInt, timeZone), format, timeZone),
        TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeYearLast(yearList(i).toInt, timeZone), format, timeZone))
    }
    result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeYearFirst(yearList(yearList.size - 1).toInt, timeZone), format, timeZone), end)
    result
  }

  /**
    * 对给定的时间范围来按照月划分片段，例如："2014-01-03"到"2015-06-20"
    * 划分的片段为：(2014-01-03,2014-01-31), (2014-02-01,2014-02-28), …… ,(2015-05-01,2015-05-31), (2015-06-01,2015-06-20)
    *
    * @param start 起始时间 yyyy-MM-dd
    * @param end 终止时间 yyyy-MM-dd
    * @return 月片段序列
    */
  def getMonthSegment(start: String,
                      end: String,
                      monthFormat: String,
                      format: String,
                      timeZone: String): Seq[(String, String)] = {
    val monthList = getMonth(start, end, monthFormat, format, timeZone)
    val result = monthList.size match {
      case 0 => sys.error(" Can not get date information! ")
      case 1 => Seq(Tuple2(start, end))
      case _ => monthSegment(start, end, monthList, monthFormat, format, timeZone)
    }
    result
  }

  /**
    * 对给定的时间戳范围来按照月划分片段，例如："2014-01-03 08:01:00"到"2015-06-20 15:00:05"
    * 划分的片段为：(2014-01-03 08:01:00,2014-01-31 23:59:59), (2014-02-01 00:00:00,2014-02-28 23:59:59), …… ,
    * (2015-05-01 00:00:00,2015-05-31 23:59:59), (2015-06-01 00:00:00,2015-06-20 15:00:05)
    *
    * @param start 起始时间 yyyy-MM-dd HH:mm:ss
    * @param end 终止时间 yyyy-MM-dd HH:mm:ss
    * @return 月片段序列
    */
  def getTimeMonthSegment(start: String,
                          end: String, monthFormat: String, format: String, timeZone: String): Seq[(String, String)] = {
    val monthList = getMonth(start, end, monthFormat, format, timeZone)
    val result = monthList.size match {
      case 0 => sys.error(" Can not get date information! ")
      case 1 => Seq(Tuple2(start, end))
      case _ => monthTimeSegment(start, end, monthList, monthFormat, format, timeZone)
    }
    result
  }

  /**
    * 对给定的时间范围来按照月划分片段
    *
    * @param start 起始时间 yyyy-MM-dd
    * @param end 终止时间 yyyy-MM-dd
    * @param monthList 月序列
    * @return 月片段序列
    */
  def monthSegment(start: String,
                   end: String,
                   monthList: Seq[String],
                   monthFormat: String,
                   format: String,
                   timeZone: String): Seq[(String, String)] = {
    val result = new ListBuffer[(String, String)]
    result += Tuple2(start, TimeUnifyUtil.formatDate(TimeUnifyUtil.getMonthLast(monthList(0), monthFormat, timeZone), format, timeZone))
    //第二个元素到倒数第二个元素
    for (i <- 1 to monthList.size - 2) {
      result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getMonthFirst(monthList(i), monthFormat, timeZone), format, timeZone),
        TimeUnifyUtil.formatDate(TimeUnifyUtil.getMonthLast(monthList(i), monthFormat, timeZone), format, timeZone))
    }
    result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getMonthFirst(monthList(monthList.size - 1), monthFormat, timeZone), format, timeZone), end)
    result
  }

  /**
    * 对给定的时间戳范围来按照月划分片段
    *
    * @param start 起始时间 yyyy-MM-dd HH:mm:ss
    * @param end 终止时间 yyyy-MM-dd HH:mm:ss
    * @param monthList 月序列
    * @return 月片段序列
    */
  def monthTimeSegment(start: String,
                       end: String,
                       monthList: Seq[String],
                       monthFormat: String,
                       format: String,
                       timeZone: String): Seq[(String, String)] = {
    val result = new ListBuffer[(String, String)]
    result += Tuple2(start, TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeMonthLast(monthList(0), monthFormat, timeZone), format, timeZone))
    //第二个元素到倒数第二个元素
    for (i <- 1 to monthList.size - 2) {
      result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeMonthFirst(monthList(i), monthFormat, timeZone), format, timeZone),
        TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeMonthLast(monthList(i), monthFormat, timeZone), format, timeZone))
    }
    result += Tuple2(TimeUnifyUtil.formatDate(TimeUnifyUtil.getTimeMonthFirst(monthList(monthList.size - 1), monthFormat, timeZone), format, timeZone), end)
    result
  }


  def getYear(start: String, end: String, yearFormat: String, format: String, timeZone: String): Seq[String] = {
    TimeUnifyUtil.getDate(start, end, yearFormat, format, timeZone)
  }

  def getMonth(start: String, end: String, monthFormat: String, format: String, timeZone: String): Seq[String] = {
    TimeUnifyUtil.getDate(start, end, monthFormat, format, timeZone)
  }

  def getDay(start: String, end: String, dateFormat: String, format: String, timeZone: String): Seq[String] = {
    TimeUnifyUtil.getDate(start, end, dateFormat, format, timeZone)
  }

  def getHour(start: String, end: String, hourFormat: String, format: String, timeZone: String): Seq[String] = {
    TimeUnifyUtil.getDateHour(start, end, hourFormat, format, timeZone)
  }

  def getMinute(start: String, end: String, minuteFormat: String, format: String, timeZone: String): Seq[String] = {
    TimeUnifyUtil.getDateMinute(start, end, minuteFormat, format, timeZone)
  }

  def getSecond(start: String, end: String, secondFormat: String, format: String, timeZone: String): Seq[String] = {
    TimeUnifyUtil.getDateSecond(start, end, secondFormat, format, timeZone)
  }


  /**
    * 低层生成高层date
    */
  def highLevelBaseLow(dateList: Array[String], format: String, timeZone: String): mutable.HashMap[String, mutable.TreeSet[String]] = {
    val mapDate = new mutable.HashMap[String, mutable.TreeSet[String]]()
    for (date <- dateList) {
      val fDate = TimeUnifyUtil.formatDate(TimeUnifyUtil.parseToDate(date, format, timeZone), format, timeZone)
      if (mapDate.contains(fDate)) {
        val arrayDates = mapDate(fDate)
        arrayDates += date
        mapDate.put(fDate, arrayDates)
      } else {
        mapDate.put(fDate, mutable.TreeSet[String](date))
      }
    }
    mapDate
  }

  //判断level合法
  def checkLevel(minLevel: String): Boolean = {
    LEVELS.contains(minLevel)
  }

  //对比获取新的level序列，主要来判断cube tree只支持到D的问题
  def newLevels(minLevel: String): Seq[String] = {
    var newLevels = Seq[String]()
    val levels = TimeUtil.LEVELS
    //得到需要划分的时间层次
    if (timeLevels.contains(minLevel)) {
      newLevels = levels.slice(0, levels.indexOf(TimeUnifyUtil.DAY) + 1)
      newLevels = newLevels :+ minLevel
    } else {
      newLevels = levels.slice(0, levels.indexOf(minLevel) + 1)
    }
    newLevels
  }

  /**
    * 查询时间范围和cube时间范围比较，得到查询时间范围可以进行cube查询的范围是多少
    *
    * @param queryDate 查询时间范围
    * @param cubeDate cube时间范围
    * @return 可使用cube的时间范围
    */
  def cubeDateSegment(queryDate: (String, String), cubeDate: (String, String), format: String, timeZone: String): (String, String) = {
    (TimeUnifyUtil.compareDate(queryDate._1, cubeDate._1, format, timeZone), TimeUnifyUtil.compareDate(queryDate._2, cubeDate._2, format, timeZone)) match {
      case (1, 1) => Tuple2(queryDate._1, cubeDate._2)
      case (1, -1) => Tuple2(queryDate._1, queryDate._2)
      case (-1, 1) => Tuple2(cubeDate._1, cubeDate._2)
      case _ => Tuple2(cubeDate._1, queryDate._2)
    }
  }
}
