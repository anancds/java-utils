package com.cds.utils.common

import java.text.SimpleDateFormat
import java.util.TimeZone

/**
  * Created by chendongsheng5 on 2017/3/29.
  */
class TimeUnifyUtilTest extends AppSpec{

  test("test parse date") {
    val dateStr = "2016-12-01 10:55:59.555"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("GMT"))
    assertResult(1480589759555L)(format.parse(dateStr).getTime)
    assertResult("2016-12-01 10:55:59.555")(format.format(format.parse(dateStr)))


    val format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format1.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    assertResult(1480560959555L)(format1.parse(dateStr).getTime)
    assertResult("2016-12-01 10:55:59.555")(format1.format(format1.parse(dateStr)))
  }
}
