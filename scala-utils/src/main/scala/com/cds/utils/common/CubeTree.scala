package com.cds.utils.common

import java.util.regex.{Matcher, Pattern}

import com.cds.utils.{CubeJUtil, CubeUtil}
import com.cds.utils.common.hierar.TimeHierarClassifier
import com.cds.utils.mgr.CubeNodeStatus

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by chendongsheng5 on 2017/3/24.
  */
class CubeTree() extends Serializable with Logging {

  //逻辑树
  // {节点名称, 节点对象}
  var CUBE_TREE = new mutable.HashMap[String, CubeNode]
  //年字典 <year, ID> 例如：(2011 -> 00)
  var YEAR_DICTIONARY = new mutable.LinkedHashMap[String, String]()

  var MIN_LEVEL = ""
  //真实时间范围
  var DATE_SEGMENT = ("", "")

  var datePrefix: String = null

  // 不可信节点是否可以查询开关
  var reliabilitySwitch = true

  //时间信息
  var format = ""
  var timeFormatSeq: Seq[String] = Seq.empty
  var timeZone = ""

  private val cubeNodeLock = new Array[Byte](0)

  // 立方体节点操作锁
  def this(start: String,
           end: String,
           minLevel: String,
           timeFormatSeq:Seq[String],
           timeFormat: String,
           timeZone: String) {
    this()
    this.timeFormatSeq = timeFormatSeq
    this.format = timeFormat
    this.timeZone=timeZone

    buildYearID(start, end)
    val timeLevels = Seq(TimeUnifyUtil.HOUR, TimeUnifyUtil.MINUTE, TimeUnifyUtil.SECOND)
    var tier = ""
    if(timeLevels.contains(minLevel))
      tier = TimeUnifyUtil.DAY
    else
      tier = minLevel
    MIN_LEVEL = tier
  }

  //init tear dictionary
  def buildYearID(start: String, end: String) = {
    val years = TimeUtil.getYear(start, end, timeFormatSeq(0), format, timeZone)
    for (year <- years) {
      YEAR_DICTIONARY.put(year, CubeUtil.formatNum(years.indexOf(year)))
    }
  }

  //get year id
  def getYearID(timePoint: String): String = {
    val year = TimeUnifyUtil.formatDate(TimeUnifyUtil.parseToDate(timePoint, timeFormatSeq(0), timeZone), timeFormatSeq(0), timeZone)
    YEAR_DICTIONARY(year)
  }

  def addYearID(year: String, id: String) = {
    YEAR_DICTIONARY.put(year, id)
  }

  /**
    * 按照层次生成cube tree. 从最底层逐渐向上生成
    *
    * @param dateList 最底层date list
    * @param tier 层级标识
    */
  def buildCubeTree(dateList: Array[String], tier: String): Unit = {
    var newLevels = Seq[String]()
    val levels = TimeUtil.LEVELS
    //得到需要划分的时间层次
    newLevels = levels.slice(0, levels.indexOf(tier))
    //最低层节点更新
    addNodeViaList(dateList, tier)
    //通过最底层更新上层
    newLevels.size match {
      case 1 =>
        addNodeViaMap(dateList, TimeUnifyUtil.YEAR)
        log.info(" Init logic tree success! ")
      case 2 =>
        addNodeViaMap(dateList, TimeUnifyUtil.MONTH)
        log.info(" Init logic tree success! ")
      case _ => log.info(" Init logic tree success! ")
    }
  }

  /**
    * 得到所有非usuable状态
    */
  def getNodeStatusUnusable(start: String, end: String, minLevel: String): (Seq[String], Seq[String], Seq[String]) ={
    val unusable = getNodeStatus(start, end, minLevel, CubeNodeStatus.UNUSABLE)
    val deprecated = getNodeStatus(start, end, minLevel, CubeNodeStatus.DEPRECATED)
    val invalid = getNodeStatus(start, end, minLevel, CubeNodeStatus.INVALID)
    reliabilitySwitch match {
      case true => (unusable._1 ++ deprecated._1 ++ invalid._1, unusable._2 ++ deprecated._2 ++ invalid._2, unusable._3 ++ deprecated._3 ++ invalid._3)
      case false =>
        val reliability = getNodeReliability(start, end, minLevel)
        ((unusable._1 ++ deprecated._1 ++ invalid._1 ++ reliability._1).distinct, (unusable._2 ++ deprecated._2 ++ invalid._2 ++ reliability._2).distinct,
          (unusable._3 ++ deprecated._3 ++ invalid._3 ++ reliability._3).distinct)
    }
  }

  /**
    * 根据时间范围，minLevel和要获取的状态来得到先要节点状态的列表
    *
    * @param start 起始时间
    * @param end 终止时间
    * @param minLevel 最小时间层
    * @param status 获取状态参数
    * @return
    */
  def getNodeStatus(start: String, end: String, minLevel: String, status: CubeNodeStatus): (Seq[String], Seq[String], Seq[String]) = {
    val yearsList = new ListBuffer[String]
    val monthsList = new ListBuffer[String]
    val daysList = new ListBuffer[String]
    val years = TimeUtil.getYear(start, end, timeFormatSeq(0), format, timeZone)
    for (year <- years) {
      val value = if (CUBE_TREE.get(year).isEmpty) CubeNodeStatus.INVALID else CUBE_TREE.get(year).get.getStatus
      //如果map获取key为null时，赋值时为0，可能与unusable重叠
      if (CubeNodeStatus.INVALID != value && value == status) {
        yearsList += year
      }
    }
    if (!minLevel.equals(TimeUnifyUtil.YEAR)) {
      val months = TimeUtil.getMonth(start, end, timeFormatSeq(1), format, timeZone)
      for (month <- months) {
        val value = if (CUBE_TREE.get(month).isEmpty) CubeNodeStatus.INVALID else CUBE_TREE.get(month).get.getStatus
        if (CubeNodeStatus.INVALID != value && value == status) {
          monthsList += month
        }
      }
      if (!minLevel.equals(TimeUnifyUtil.MONTH)) {
        val days = TimeUtil.getDay(start, end, timeFormatSeq(2), format, timeZone)
        for (day <- days) {
          val value = if (CUBE_TREE.get(day).isEmpty) CubeNodeStatus.INVALID else CUBE_TREE.get(day).get.getStatus
          if (CubeNodeStatus.INVALID != value && value == status) {
            daysList += day
          }
        }
      }
    }
    (yearsList, monthsList, daysList)
  }

  /**
    * 查询cube node不可信状态节点
    */
  def getNodeReliability(start: String, end: String, minLevel: String): (Seq[String], Seq[String], Seq[String]) ={
    val yearsList = new ListBuffer[String]
    val monthsList = new ListBuffer[String]
    val daysList = new ListBuffer[String]
    val years = TimeUtil.getYear(start, end, timeFormatSeq(0), format, timeZone)
    for (year <- years) {
      if(!CUBE_TREE.get(year).isEmpty && CUBE_TREE.get(year).get.getReliability !=0)
        yearsList += year
    }
    if (!minLevel.equals(TimeUnifyUtil.YEAR)) {
      val months = TimeUtil.getMonth(start, end, timeFormatSeq(1), format, timeZone)
      for (month <- months) {
        if (!CUBE_TREE.get(month).isEmpty && CUBE_TREE.get(month).get.getReliability !=0)
          monthsList += month
      }
      if (!minLevel.equals(TimeUnifyUtil.MONTH)) {
        val days = TimeUtil.getDay(start, end, timeFormatSeq(2), format, timeZone)
        for (day <- days) {
          if (!CUBE_TREE.get(day).isEmpty && CUBE_TREE.get(day).get.getReliability !=0)
            daysList += day
        }
      }
    }
    (yearsList, monthsList, daysList)
  }

  /**
    * 根据节点名称获取子立方体节点对象
    *
    * @param nodeName 节点名称
    */
  def getCubeNodeViaName(nodeName: String): CubeNode = {

    cubeNodeLock.synchronized {
      CUBE_TREE.get(nodeName).get
    }
  }

  /**
    * 根据单个时间点获取时间点隶属的最小层级的子立方体对象
    *
    * @param timePoint 时间点参数，date为字符串，timestamp为long
    */
  def getCubeNodeViaTimePoint(timePoint: Any): CubeNode = {

    cubeNodeLock.synchronized {
      val date = getDate(timePoint)
      CUBE_TREE.getOrElse(date, null)
    }
  }

  def getMinLevelCubeNodes: ListBuffer[CubeNode] = {

    val cubeNodeList = new ListBuffer[CubeNode]
    CUBE_TREE.foreach(kv => {
      if (kv._2.getTier == MIN_LEVEL)
        cubeNodeList += kv._2
    })

    cubeNodeList
  }

  /**
    * 根据时间来添加最底层CubeNode节点，更新之后联动判断上层节点是否需要更新
    * 判断year dictionary是否需要更新
    *
    * @param timePoint 时间点参数
    * @return CubeNode
    */
  def addNodeViaTimePoint(timePoint: Any, cubeInfo: CubeInfo): CubeNode = {

    cubeNodeLock.synchronized {
      val date = getDate(timePoint)
      if (CUBE_TREE.keySet.contains(date)) {
        sys.error(" Date has exist in cube tree. ")
      }

      //判断是否更新year dictionary
      addYearID(timePoint)

      val cubeNode = new CubeNode(date)
      cubeNode.setTier(MIN_LEVEL)
      cubeNode.setStart(date)
      cubeNode.setEnd(date)
      cubeNode.setStatus(CubeNodeStatus.USABLE)
      addNodeViaName(date, cubeNode)

      if(MIN_LEVEL != TimeUnifyUtil.YEAR){
        MIN_LEVEL match {
          case TimeUnifyUtil.DAY => compareNodeDate(date, TimeUnifyUtil.MONTH)
          case TimeUnifyUtil.MONTH => compareNodeDate(date, TimeUnifyUtil.YEAR)
        }
      }

      val formatDate = MIN_LEVEL match {
        case TimeUnifyUtil.DAY => TimeUnifyUtil.formatDay(date, timeFormatSeq(2), timeZone)
        case TimeUnifyUtil.MONTH => TimeUnifyUtil.formatDate(TimeUnifyUtil.getMonthLast(date, timeFormatSeq(1), timeZone), timeFormatSeq(2), timeZone)
        case TimeUnifyUtil.YEAR => TimeUnifyUtil.formatDate(TimeUnifyUtil.getYearLast(date.substring(0, 4).toInt, timeZone), timeFormatSeq(2), timeZone)
      }

      if(!DATE_SEGMENT._2.equals("")){
        if(TimeUnifyUtil.compareDate(DATE_SEGMENT._2, formatDate, format, timeZone) == -1)
          DATE_SEGMENT = (DATE_SEGMENT._1, formatDate)
      }else
        DATE_SEGMENT = (DATE_SEGMENT._1, formatDate)

      if(!DATE_SEGMENT._1.equals("")){
        if(TimeUnifyUtil.compareDate(DATE_SEGMENT._1, formatDate, format, timeZone) == 1)
          DATE_SEGMENT = (formatDate, DATE_SEGMENT._2)
      }else
        DATE_SEGMENT = (formatDate, DATE_SEGMENT._2)

      updateTreeFile(cubeInfo)
      cubeNode
    }
  }

  def updateNodeReliability(timePoint: Any, reliability: Int): Unit ={
    //更新最底层的node status
    val date = getDate(timePoint)
    val cubeNode = CUBE_TREE(date)
    cubeNode.setReliability(reliability)
  }

  /**
    * update node status
    * only update min level node status
    * higher level status update based on min level status
    *
    * @param timePoint 时间点参数
    * @param status CubeNodeStatus
    */
  def updateNodeStatus(cubeInfo: CubeInfo, timePoint: Any, status: CubeNodeStatus) = {
    //更新最底层的node status
    val date = getDate(timePoint)
    val cubeNode = CUBE_TREE(date)
    cubeNode.setStatus(status)

    //检查上层node status是否需要更新
    if(cubeInfo.MIN_LEVEL != TimeUnifyUtil.YEAR)
      updateHigherStatus(date, status)
    //    updateTreeFile(date, cubeInfo)
  }

  /**
    * update higher status
    *
    * @param date mim level date
    */
  def updateHigherStatus(date: String, status: CubeNodeStatus): Unit = {
    val datePrefix = date.substring(0, date.length - 2)
    val pattern: Pattern = Pattern.compile("^" + datePrefix + ".*")
    var unusable = false
    var usable = true
    val iterator = CUBE_TREE.iterator
    var level = ""
    //higher node
    val higherNode = CUBE_TREE(datePrefix.substring(0, datePrefix.length - 1))
    while (iterator.hasNext && !unusable) {
      val value = iterator.next()
      val date = value._1
      val cubeNode = value._2
      val matcher: Matcher = pattern.matcher(date)
      if (matcher.matches()) {
        if(status.equals(CubeNodeStatus.USABLE)){
          if (cubeNode.getStatus.equals(CubeNodeStatus.UNUSABLE)) {
            usable = false
          }
        }else{
          if (!cubeNode.getStatus.equals(CubeNodeStatus.USABLE)) {
            if (!higherNode.getStatus.equals(CubeNodeStatus.UNUSABLE))
              higherNode.setStatus(CubeNodeStatus.UNUSABLE)
            unusable = true
          }
        }
        level = higherNode.getTier
      }
    }
    if(status == CubeNodeStatus.USABLE && usable){
      higherNode.setStatus(CubeNodeStatus.USABLE)
    }

    if (!level.equals(TimeUnifyUtil.YEAR))
      updateHigherStatus(datePrefix.substring(0, datePrefix.length - 1), status)
  }

  //格式化timePoint，将timePoint转化为minLevel层的date
  def getDate(timePoint: Any): String = {
    val dateValue = MIN_LEVEL match {
      case TimeUnifyUtil.YEAR =>
        val time = new TimeHierarClassifier(timePoint, "yyyy", timeZone)
        time.getYearHierarchy
      case TimeUnifyUtil.MONTH =>
        val time = new TimeHierarClassifier(timePoint, "yyyy-MM", timeZone)
        time.getMonthHierarchy
      case TimeUnifyUtil.DAY =>
        val time = new TimeHierarClassifier(timePoint, "yyyy-MM-dd", timeZone)
        time.getDateHierarchy
      case _ => sys.error(" Can not support this minLevel : " + MIN_LEVEL)
    }
    dateValue.substring(1)
  }

  def addNodeViaName(nodeName: String, cubeNode: CubeNode) = {

    cubeNodeLock.synchronized {
      CUBE_TREE.put(nodeName, cubeNode)
    }
  }

  //创建cube时批量创建最底层CubeNode
  def addNodeViaList(dateList: Array[String], tier: String) = {
    for (date <- dateList) {
      val cubeNode = new CubeNode(date)
      cubeNode.setTier(tier)
      cubeNode.setStart(date)
      cubeNode.setEnd(date)
      cubeNode.setStatus(CubeNodeStatus.USABLE)
      addNodeViaName(date, cubeNode)
    }
  }

  //创建cube时根据最底层list批量创建上层CubeNode
  def addNodeViaMap(dateList: Array[String], tier: String): Unit = {
    val dateMap = tier match {
      case TimeUnifyUtil.YEAR => TimeUtil.highLevelBaseLow(dateList, timeFormatSeq(0), timeZone)
      case TimeUnifyUtil.MONTH => TimeUtil.highLevelBaseLow(dateList, timeFormatSeq(1), timeZone)
    }

    for ((highDate, lowDates) <- dateMap) {
      val cubeNode = new CubeNode(highDate)
      cubeNode.setTier(tier)
      cubeNode.setStart(lowDates.firstKey)
      cubeNode.setEnd(lowDates.lastKey)
      cubeNode.setStatus(CubeNodeStatus.USABLE)
      addNodeViaName(highDate, cubeNode)
    }
    if (!tier.equals(TimeUnifyUtil.YEAR))
      addNodeViaMap(dateMap.keySet.toArray, TimeUnifyUtil.YEAR)
  }

  /**
    * update节点时判断高层次节点的时间范围是否需要更新，需要时更新
    *
    * @param date 低层节点时间
    * @param tier 时间层次
    */
  def compareNodeDate(date: String, tier: String): Unit = {
    var format = ""
    val highDate = tier match {
      case TimeUnifyUtil.YEAR =>
        format = timeFormatSeq(1)
        TimeUnifyUtil.formatDate(TimeUnifyUtil.parseToDate(date, timeFormatSeq(0), timeZone), timeFormatSeq(0), timeZone)
      case _ =>
        format = timeFormatSeq(2)
        TimeUnifyUtil.formatDate(TimeUnifyUtil.parseToDate(date, timeFormatSeq(1), timeZone), timeFormatSeq(1), timeZone)
    }

    if (CUBE_TREE.get(highDate).equals(None)) {
      val cubeNode = new CubeNode(highDate)
      cubeNode.setTier(tier)
      cubeNode.setStart(date)
      cubeNode.setEnd(date)
      cubeNode.setStatus(CubeNodeStatus.USABLE)
      addNodeViaName(highDate, cubeNode)
    } else {
      val node = CUBE_TREE.get(highDate).get
      if (TimeUnifyUtil.compareDate(node.getStart, date, format, timeZone) == 1) {
        node.setStart(date)
        CUBE_TREE.put(highDate, node)
      }
      if (TimeUnifyUtil.compareDate(node.getEnd, date, format, timeZone) == -1) {
        node.setEnd(date)
        CUBE_TREE.put(highDate, node)
      }
    }
    if (!tier.equals(TimeUnifyUtil.YEAR))
      compareNodeDate(highDate, TimeUnifyUtil.YEAR)
  }

  def dropNode(date: String) = {

    cubeNodeLock.synchronized {
      //val date = getDate(timePoint)
      CUBE_TREE.remove(date)
    }
  }

  /**
    * 判断是否更新year dictionary
    *
    * @param timePoint 时间点参数，date或timestamp的字符串形式
    * @return
    */
  def addYearID(timePoint: Any) = {
    val date = getDate(timePoint)
    val year = TimeUnifyUtil.formatDate(TimeUnifyUtil.parseToDate(date, timeFormatSeq(0), timeZone), timeFormatSeq(0), timeZone)
    if (!YEAR_DICTIONARY.keySet.contains(year)) {
      var max = 0
      for ((year, id) <- YEAR_DICTIONARY) {
        if (max < id.toInt)
          max = id.toInt
      }
      val Id = CubeUtil.formatNum(max + 1)
      YEAR_DICTIONARY.put(year, Id)
    }
  }

  def dropYearID(year: String) = {
    YEAR_DICTIONARY.remove(year)
  }

  /**
    * update, load to hdfs new tree
    */
  def updateTreeFile(cubeInfo: CubeInfo): Unit = {
    cubeNodeLock.synchronized {
      CubeJUtil.treeToJson(this, cubeInfo.hdfsBasePath, cubeInfo.CUBE_NAME, cubeInfo.udeTableName, cubeInfo.newPath)
      logTrace(cubeInfo.CUBE_NAME + " finish update tree file of " + cubeInfo.udeTableName + ".")
    }
  }

  def addTimeSeq(time: String){
    timeFormatSeq = timeFormatSeq :+ time
  }

  def showCubeTree(): String ={
    val result = new mutable.StringBuilder()
    CUBE_TREE.map(r => {
      result.append(" node: " + r._2.getNodeName + " : {" + " status: " + r._2.getStatus + " ,reliability: " + r._2.getReliability + " },")
    })
    result.toString()
  }

  /**
    * 展示不可用状态节点
    */
  def showDisableNodes(): String={
    val disableNode = getNodeStatusUnusable(DATE_SEGMENT._1, DATE_SEGMENT._2, MIN_LEVEL)
    val disReliability = getNodeReliability(DATE_SEGMENT._1, DATE_SEGMENT._2, MIN_LEVEL)
    val disableNodes = ((disableNode._1 ++ disReliability._1).distinct, (disableNode._2 ++ disReliability._2).distinct, (disableNode._3 ++ disReliability._3).distinct)
    val result = new mutable.StringBuilder()
    result.append("year: ")
    disableNodes._1.map(r => {
      result.append(r + ",")
    })
    if(disableNodes._1.size != 0)
      result.deleteCharAt(result.length - 1)
    result.append("; ")
    result.append("month: ")
    disableNodes._2.map(r => {
      result.append(r + ",")
    })
    if(disableNodes._2.size != 0)
      result.deleteCharAt(result.length - 1)
    result.append("; ")
    result.append("day: ")
    disableNodes._3.map(r => {
      result.append(r + ",")
    })
    if(disableNodes._3.size != 0)
      result.deleteCharAt(result.length - 1)
    result.append("; ")
    result.toString()
  }
}

