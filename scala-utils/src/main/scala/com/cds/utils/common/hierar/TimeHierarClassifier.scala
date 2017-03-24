package com.cds.utils.common.hierar

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate

/**
  * Created by chendongsheng5 on 2017/3/24.
  */
class TimeHierarClassifier {

  private val frontSeparator = TimeHierarClassifier.frontSeparator
  private val middleSeparator = TimeHierarClassifier.middleSeparator
  private val backSeparator = TimeHierarClassifier.backSeparator

  private var lowestHierarchy: String = null

  private var year: String = TimeHierarClassifier.year
  private var month: String = TimeHierarClassifier.month
  private var date: String = TimeHierarClassifier.date
  private var hour: String = TimeHierarClassifier.hour
  private var minute: String = TimeHierarClassifier.minute
  private var second: String = TimeHierarClassifier.second


  // 时间格式：yyyy-MM-dd HH:mm:ss
  // 支持Long、SQLTimestamp、SQLDate三种类型，其中SQLTimestamp等价于Long
  def this(time: Any, timeFormat: String, timeZone: String) {

    this()

    val format = "%02d"
    time match {
      // SQLTimestamp等价于Long，走同一路径
      case value: Long =>
        val tempDate = new Date(value)
        val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
        cal.setTime(tempDate)
        var tempValue = cal.get(Calendar.YEAR) + """"""
        year += tempValue
        tempValue = tempValue + frontSeparator + String.format(format, new Integer(cal.get(Calendar.MONTH) + 1))
        month += tempValue
        tempValue += frontSeparator + String.format(format, new Integer(cal.get(Calendar.DATE)))
        date += tempValue
        tempValue += middleSeparator + String.format(format, new Integer(cal.get(Calendar.HOUR_OF_DAY)))
        hour += tempValue
        tempValue += backSeparator + String.format(format, new Integer(cal.get(Calendar.MINUTE)))
        minute += tempValue
        tempValue += backSeparator + String.format(format, new Integer(cal.get(Calendar.SECOND)))
        second += tempValue
        lowestHierarchy = TimeHierarClassifier.secondHierarchy
      //        println(second)
      case value: SQLDate =>
        var tempValue = DateTimeUtils.getYear(value) + """"""
        year += tempValue
        tempValue += frontSeparator + String.format(format, new Integer(DateTimeUtils.getMonth(value)))
        month += tempValue
        tempValue += frontSeparator + String.format(format, new Integer(DateTimeUtils.getDayOfMonth(value)))
        date += tempValue
        lowestHierarchy = TimeHierarClassifier.dateHierarchy
      //        println(date)
      case value: java.sql.Date =>
        val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
        cal.setTime(value)
        var tempValue = cal.get(Calendar.YEAR) + """"""
        year += tempValue
        tempValue = tempValue + frontSeparator + String.format(format, new Integer(cal.get(Calendar.MONTH) + 1))
        month += tempValue
        tempValue += frontSeparator + String.format(format, new Integer(cal.get(Calendar.DATE)))
        date += tempValue
        lowestHierarchy = TimeHierarClassifier.dateHierarchy
      //        println(date)
      case value: java.sql.Timestamp =>
        val tempDate = new Date(value.getTime)
        val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
        cal.setTime(tempDate)
        var tempValue = cal.get(Calendar.YEAR) + """"""
        year += tempValue
        tempValue = tempValue + frontSeparator + String.format(format, new Integer(cal.get(Calendar.MONTH) + 1))
        month += tempValue
        tempValue += frontSeparator + String.format(format, new Integer(cal.get(Calendar.DATE)))
        date += tempValue
        tempValue += middleSeparator + String.format(format, new Integer(cal.get(Calendar.HOUR_OF_DAY)))
        hour += tempValue
        tempValue += backSeparator + String.format(format, new Integer(cal.get(Calendar.MINUTE)))
        minute += tempValue
        tempValue += backSeparator + String.format(format, new Integer(cal.get(Calendar.SECOND)))
        second += tempValue
        lowestHierarchy = TimeHierarClassifier.secondHierarchy
      //        println(date)
      case value: String =>
        if (!value.contains(":")) {
          try {
            val tempFormat = new SimpleDateFormat(timeFormat)
            tempFormat.setTimeZone(TimeZone.getTimeZone(timeZone))
            val tempDate = tempFormat.parse(value)
            val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
            cal.setTime(tempDate)
            var tempValue = cal.get(Calendar.YEAR) + """"""
            year += tempValue
            tempValue = tempValue + frontSeparator + String.format(format, new Integer(cal.get(Calendar.MONTH) + 1))
            month += tempValue
            tempValue += frontSeparator + String.format(format, new Integer(cal.get(Calendar.DATE)))
            date += tempValue
            lowestHierarchy = TimeHierarClassifier.dateHierarchy
            //            println(date)
          } catch {
            case e: Exception => sys.error("unsupported time type exception: " + e + ", value =" + value)
          }
        } else {
          try {
            val timeStamp = Timestamp.valueOf(value)
            val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
            cal.setTime(timeStamp)
            var tempValue = cal.get(Calendar.YEAR) + """"""
            year += tempValue
            tempValue = tempValue + frontSeparator + String.format(format, new Integer(cal.get(Calendar.MONTH) + 1))
            month += tempValue
            tempValue += frontSeparator + String.format(format, new Integer(cal.get(Calendar.DATE)))
            date += tempValue
            tempValue += middleSeparator + String.format(format, new Integer(cal.get(Calendar.HOUR_OF_DAY)))
            hour += tempValue
            tempValue += backSeparator + String.format(format, new Integer(cal.get(Calendar.MINUTE)))
            minute += tempValue
            tempValue += backSeparator + String.format(format, new Integer(cal.get(Calendar.SECOND)))
            second += tempValue
            lowestHierarchy = TimeHierarClassifier.secondHierarchy
            //            println(second)
          } catch {
            case e: Exception => sys.error("unsupported time type exception: " + e + ", value =" + value)
          }
        }

      case _ => sys.error("unsupported time type exception: " + time)

    }
  }

  /**
    * @return 最低层级表示
    */
  def getLowestHierarchy: String = {

    lowestHierarchy
  }

  def getSpecifiedHierarchy(hierarchy: String): String = {

    hierarchy match {
      case TimeHierarClassifier.yearHierarchy => year
      case TimeHierarClassifier.monthHierarchy => month
      case TimeHierarClassifier.dateHierarchy => date
      case TimeHierarClassifier.hourHierarchy => hour
      case TimeHierarClassifier.minuteHierarchy => minute
      case TimeHierarClassifier.secondHierarchy => second
      case _ => sys.error("unsupported time hierarchy exception: " + hierarchy)
    }
  }

  def getYearHierarchy: String = {
    year
  }

  def getMonthHierarchy: String = {
    month
  }

  def getDateHierarchy: String = {
    date
  }

  def getHourHierarchy: String = {
    hour
  }

  def getMinuteHierarchy: String = {
    minute
  }

  def getSecondHierarchy: String = {
    second
  }
}

object TimeHierarClassifier {

  private val yearHierarchy: String = "year"
  private val monthHierarchy: String = "month"
  private val dateHierarchy: String = "date"
  private val hourHierarchy: String = "hour"
  private val minuteHierarchy: String = "minute"
  private val secondHierarchy: String = "second"

  private val frontSeparator = "-"
  private val middleSeparator = " "
  private val backSeparator = ":"

  private val year: String = "Y"
  private val month: String = "M"
  private val date: String = "D"
  private val hour: String = "H"
  private val minute: String = "F"
  private val second: String = "S"

  def timeToStringType(classifiedTimeValue: String, timeType: String): String = {

    val hierarchyFlag = classifiedTimeValue.charAt(0).toString
    val timeValueStr = classifiedTimeValue.substring(1)

    // todo:当前只处理到最底层, 层次类型尚未处理
    hierarchyFlag match {
      case TimeHierarClassifier.date =>
        timeType match {
          case "Date" => timeValueStr
          case _ => sys.error("unsupported time hierarchy exception: dateType = " + timeType)
        }
      case TimeHierarClassifier.second =>
        timeType match {
          case "Timestamp" => timeValueStr
          //            val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeValueStr)
          //            date.getTime.toString
          case "Long" =>
            val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeValueStr)
            date.getTime.toString
          case _ => sys.error("unsupported time hierarchy exception: dateType = " + timeType)
        }
      // todo 为了过滤掉非最低层时间维度层次增加，需要改进
      case _ => null //sys.error("unsupported time hierarchy exception: classifiedTimeValue = " + classifiedTimeValue)
    }
  }


  def main(args: Array[String]) = {


    //    new TimeHierarClassifier(System.currentTimeMillis())
    //    new TimeHierarClassifier(1451983500000l)
    //    new TimeHierarClassifier(1451980800000l)
    //    new TimeHierarClassifier(1451923200000l)
    //    new TimeHierarClassifier(1451970000000l)
    //
    //
    //    new TimeHierarClassifier(1451491200000l)
    //
    //    new TimeHierarClassifier(DateTimeUtils.fromJavaTimestamp(new Timestamp(1451983500000l)))
    //
    //    new TimeHierarClassifier(DateTimeUtils.millisToDays(1451983500000l))
    //    new TimeHierarClassifier(DateTimeUtils.millisToDays(1451980800000l))
    //    new TimeHierarClassifier(DateTimeUtils.millisToDays(1451491200000l))
    //    println(new TimeHierarClassifier(sql.Date.valueOf("2015-01-02")).getMonthHierarchy)
    //
    //    println(new TimeHierarClassifier("2015-01-02"))
    //    println(new TimeHierarClassifier("2015-01-02 9:12:56"))

    //    println(new TimeHierarClassifier(new Timestamp(1454481670000L)).getSecondHierarchy)
    println("test")
  }

}
