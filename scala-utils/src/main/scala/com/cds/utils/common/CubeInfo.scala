package com.cds.utils.common

import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONArray}
import com.cds.utils.update.CubeUpdateInfo
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{count, max, min, sum}
import org.quartz.CronExpression

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chendongsheng5 on 2017/3/24.
  */
class CubeInfo extends Logging {

  var allDimension: ArrayBuffer[Array[String]] = ArrayBuffer[Array[String]]()
  var allMeasure: ArrayBuffer[(String, Array[String])] = ArrayBuffer[(String, Array[String])]()
  var showDimension: ArrayBuffer[String] = ArrayBuffer[String]()

  var cubeTree: CubeTree = _
  var cubeUpdateInfo: CubeUpdateInfo = _
  // colTypeMap: [列名, (列类型, 对应HBase列名(family:qualifier))]
  var colMap: Map[String, (String, (String))] = null
  // [列名, (family, qualifier, 列类型)]Map
  var ColsInfo: Map[String, (Array[Byte], Array[Byte], String)] = null

  //cube 用户定义目录
  var zkAdd: String = ""
  var newPath = ""
  var hbaseTableName = ""
  var hdfsBasePath = ""
  var datePrefix: String = _
  var udeTableName = ""
  //hbp Schema info
  var hbpSchema: Schema = _
  var hbpGroup: Object = _

  var CUBE_NAME: String = ""
  var cubeHBaseTableName: String = ""
  //维度映射 <ID, 维度名> 例如：(stri -> 01, bool -> 02, date -> 03)
  var DIMENSIONS = Map[String, String]()
  //时间段
  var CUBE_DATE: (String, String) = ("", "")
  //时间列
  var DATE_COLUMN: String = ""
  var DATE_COLUM_FORMAT: String = ""
  var DATE_COLUM_ZONE: String = ""
  var MIN_LEVEL: String = ""
  var DATE_TYPE: String = ""
  //维度树 <维度层ID, 维度名序列> 例如：(02,0200 -> ArrayBuffer(date,  stri), 03,000102 -> ArrayBuffer(stri,  bool,  date))
  var DIMENSIONS_TREE = TreeMap[String, Seq[Column]]()
  //聚合列信息 （聚合列列表， 聚合列hbase存储名称，dataFrame聚合列名称）
  var MEASURES = (Seq[String](), Seq[String](), Seq[Column]())

  //并行度
  var parallelism = 0.1

  // 是否有重复insert操作，缺省为有
  var includeReinsert = true

  // 重复录入记录（insert）时, 维度与度量列是否会改变, 缺省不会改变, 即重复insert不涉及cube联动更新
  var reinsertAffectCubeFields = false


  // 不可信节点是否可以查询开关
  var reliabilitySwitch = true
  def this(jsonString: String, tableColsInfo: java.util.Map[String, (Array[Byte], Array[Byte], String)], timeInfo: (String, String, String)) = {
    this()
    val schemaInfo = new mutable.HashMap[String, (Array[Byte], Array[Byte], String)]()
    for ((table, schema) <- tableColsInfo) {
      schemaInfo.put(table, schema)
    }
    ColsInfo = schemaInfo.toMap
    val json = JSON.parseObject(jsonString)
    val pattern = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$")
    try {
      CUBE_NAME = json.get("CubeName").toString.trim
      if (!pattern.matcher(CUBE_NAME).matches()) {
        sys.error(" Cube name is invalid. ")
      }
    } catch {
      case ex: Exception => sys.error(" Can not get CubeName, please check cube json! Exception: " + ex)
    }
    try {
      cubeHBaseTableName = json.get("HBaseTable").toString.trim
      if (!pattern.matcher(cubeHBaseTableName).matches()) {
        sys.error(" HBase table name is invalid. ")
      }
    } catch {
      case ex: Exception => sys.error(" Can not get HBaseTable, please check cube json! Exception: " + ex)
    }
    try {
      parseHierarchy(json.get("Hierarchy"), tableColsInfo, timeInfo)
    } catch {
      case ex: Exception => sys.error(" Can not get Hierarchy, please check cube json! Exception: " + ex)
    }
    try {
      parseDimensions(json.get("Dimensions").asInstanceOf[JSONArray].toArray, tableColsInfo)
    } catch {
      case ex: Exception => sys.error(" Can not get Dimensions, please check cube json! Exception: " + ex)
    }
    try {
      MEASURES = parseMeasures(json.get("Measures"), tableColsInfo)
    } catch {
      case ex: Exception => sys.error(" Can not get Measures, please check cube json! Exception: " + ex)
    }
    try {
      parseParameters(json.get("Parameters"), tableColsInfo)
    } catch {
      case ex: Exception => sys.error(" Can not get Parameters, please check cube json! Exception: " + ex)
    }
  }
  //解析分层信息 CUBE_DATE, DATE_COLUMN, MIN_LEVEL
  private def parseHierarchy(hierarchy: AnyRef, tableColsInfo: java.util.Map[String, (Array[Byte], Array[Byte], String)], timeInfo: (String, String, String)): Unit = {
    val hierarchyInfo = JSON.parseObject(hierarchy.toString)

    DATE_COLUMN = hierarchyInfo.get("ColumnName").toString

    MIN_LEVEL = hierarchyInfo.get("MinLevel").toString

    val dateType = tableColsInfo(DATE_COLUMN)._3
    if (!dateType.equals("Date") && !dateType.equals("Timestamp") && !dateType.equals("Long")) {
      sys.error("dateType should only be date or timestamp or long")
    }
    DATE_TYPE = dateType
    val timeLevels = Seq(TimeUnifyUtil.HOUR, TimeUnifyUtil.MINUTE, TimeUnifyUtil.SECOND)
    if (dateType.equals("Date") && timeLevels.contains(MIN_LEVEL)) {
      sys.error(" date column type is Date, but min level is " + MIN_LEVEL + ", min level only D, M or Y")
    }

    if (dateType.equals("Date")) {
      DATE_COLUM_FORMAT = timeInfo._1
    } else {
      DATE_COLUM_FORMAT = timeInfo._2
    }
    DATE_COLUM_ZONE = timeInfo._3

    val startDate = hierarchyInfo.get("StartDate").toString
    if (!CubeJUtil.checkValidDate(startDate, dateType)) {
      sys.error(" start date is an invalid date. ")
    }
    val endDate = hierarchyInfo.get("EndDate").toString
    if (!CubeJUtil.checkValidDate(endDate, dateType)) {
      sys.error(" end date is an invalid date. ")
    }
    if (TimeUnifyUtil.compareDate(startDate, endDate, DATE_COLUM_FORMAT, DATE_COLUM_ZONE) == 1) {
      sys.error(" start time is bigger than end time, please check it! ")
    }
    CUBE_DATE = (startDate, endDate)

    if (!TimeUtil.checkLevel(MIN_LEVEL)) {
      sys.error(" Please check your minLevel parameter, minLevel only supports Y, M, D, H, F or S! ")
    }
  }

  //解析维度信息 DIMENSIONS, DIMENSIONS_TREE
  private def parseDimensions(dimensions: Array[AnyRef], tableColsInfo: java.util.Map[String, (Array[Byte], Array[Byte], String)]): Unit = {
    generateDimensions(dimensions, tableColsInfo)
    dimensionTree(dimensions)
  }

  //DIMENSIONS 有序
  private def generateDimensions(dimensions: Array[AnyRef], tableColsInfo: java.util.Map[String, (Array[Byte], Array[Byte], String)]): Unit = {
    val baseDimensions = new mutable.LinkedHashSet[String]
    for (dimension <- dimensions) {
      if (!dimension.toString.contains(DATE_COLUMN)) {
        sys.error(" Dimension must contain hierarchy column! ")
      }
      showDimension.+=(dimension.toString)
      val values = dimension.toString.split(",").map(t => t.trim)
      allDimension.+=(values)
      for (value <- values) {
        baseDimensions += value.trim
      }
    }

    val columns = tableColsInfo.keySet
    var i = 0
    baseDimensions.foreach(t => {
      if (!columns.contains(t)) {
        sys.error("Column " + t + "do not contains this table schema! ")
      }
      DIMENSIONS += (t -> CubeUtil.formatNum(i))
      i = i + 1
    })
  }

  //DIMENSIONS_TREE 有序
  private def dimensionTree(dimensions: Array[AnyRef]): Unit = {
    for (dimension <- dimensions) {
      var dims = new ArrayBuffer[Column]()
      var key = ""
      var i = 0
      dimension.toString.split(",").map(v => {
        key += DIMENSIONS.get(v.trim).get
        dims += new Column(v.trim)
        i += 1
      })
      DIMENSIONS_TREE += (CubeUtil.formatNum(i) + CubeBaseInfo.ROWKEY_SEPARATOR + key -> dims.toSeq)
    }
  }

  //MEASURES
  private def parseMeasures(measures: AnyRef, tableColsInfo: java.util.Map[String, (Array[Byte], Array[Byte], String)]): (Seq[String], Seq[String], Seq[Column]) = {
    val aggColName = new ArrayBuffer[String]()
    val aggColBuffer = new ArrayBuffer[String]()
    val aggFunBuffer = new ArrayBuffer[Column]()

    val columns = tableColsInfo.keySet
    measures.asInstanceOf[JSONArray].toArray.map(t => {
      val json = JSON.parseObject(t.toString)
      val method = json.get("Method").toString.trim
      val cols = json.get("Column").toString.split(",").map(t => t.trim)
      allMeasure.+=((method, cols))
      cols.map(c => {
        val aggCol = c.trim
        if (!columns.contains(aggCol)) {
          sys.error("Column " + aggCol + " do not contains this table schema! ")
        }
        val aggType = tableColsInfo.get(aggCol)._3
        if (!aggColName.contains(aggCol))
          aggColName.+=(aggCol)
        method match {
          case CubeBaseInfo.MEASURE_SUM =>
            if (CubeUtil.checkMeasureFunWithDataType(method, aggType)) {
              aggColBuffer.+=(CubeBaseInfo.MEASURE_SUM + "(" + aggCol + ")")
              aggFunBuffer.+=(sum(aggCol) as (CubeBaseInfo.MEASURE_SUM + "(" + aggCol + ")"))
            } else
              sys.error(" unsupported data type for measure function, measureFun = " + method + ", dataType = " + aggType)
          case CubeBaseInfo.MEASURE_COUNT =>
            if (CubeUtil.checkMeasureFunWithDataType(method, aggType)) {
              aggColBuffer.+=(CubeBaseInfo.MEASURE_COUNT + "(" + aggCol + ")")
              aggFunBuffer.+=(count(aggCol) as (CubeBaseInfo.MEASURE_COUNT + "(" + aggCol + ")"))
            } else
              sys.error(" unsupported data type for measure function, measureFun = " + method + ", dataType = " + aggType)
          case CubeBaseInfo.MEASURE_MAX =>
            if (CubeUtil.checkMeasureFunWithDataType(method, aggType)) {
              aggColBuffer.+=(CubeBaseInfo.MEASURE_MAX + "(" + aggCol + ")")
              aggFunBuffer.+=(max(aggCol) as (CubeBaseInfo.MEASURE_MAX + "(" + aggCol + ")"))
            } else
              sys.error(" unsupported data type for measure function, measureFun = " + method + ", dataType = " + aggType)
          case CubeBaseInfo.MEASURE_MIN =>
            if (CubeUtil.checkMeasureFunWithDataType(method, aggType)) {
              aggColBuffer.+=(CubeBaseInfo.MEASURE_MIN + "(" + aggCol + ")")
              aggFunBuffer.+=(min(aggCol) as (CubeBaseInfo.MEASURE_MIN + "(" + aggCol + ")"))
            } else
              sys.error(" unsupported data type for measure function, measureFun = " + method + ", dataType = " + aggType)
          case _ => sys.error(" Aggregation function do not support! ")
        }
      })
    })
    (aggColName.sorted, aggColBuffer.sorted, aggFunBuffer)
  }

  //parameters
  private def parseParameters(parameter: AnyRef, tableColsInfo: java.util.Map[String, (Array[Byte], Array[Byte], String)]): Unit = {
    val parametersInfo = JSON.parseObject(parameter.toString)
    val parallel = parametersInfo.get("parallelism")
    if (parallel != null && parallel != "")
      parallelism = parallel.toString.toDouble
    val inReinsert = parametersInfo.get("includeReinsert")
    if (inReinsert != "")
      includeReinsert = inReinsert.toString.toBoolean
    val reAffectCubeFields = parametersInfo.get("reinsertAffectCubeFields")
    if (reAffectCubeFields != "")
      reinsertAffectCubeFields = reAffectCubeFields.toString.toBoolean
    val strategyName = parametersInfo.get(FirstInsertJudgeStrategy.FIRST_INSERT_JUDGE_STRATEGY)
    val compareFieldName = parametersInfo.get(FirstInsertJudgeStrategy.COMPARE_FIELD_NAME)
    val compareValue = parametersInfo.get(FirstInsertJudgeStrategy.COMPARE_VALUE)
    if (strategyName != "") {
      val compareFieldType = tableColsInfo.get(compareFieldName)._3
      firstInsertJudgeStrategy = CubeJUtil.parseJudgeStrategy(strategyName.toString, compareFieldName.toString, compareValue.toString, compareFieldType)
    }
    val cornExpression = parametersInfo.get(CubeUpdateInfo.CRON_EXPRESSION_PARAM)
    if (cornExpression == null || !CronExpression.isValidExpression(cornExpression.toString))
      sys.error(" Cron expression is invalid, please check it! cornExpression = " + cornExpression)
    cubeUpdateInfo = new CubeUpdateInfo(CUBE_NAME, Map(CubeUpdateInfo.CRON_EXPRESSION_PARAM -> cornExpression.toString))

    val reliabilitySwitchValue = parametersInfo.get("reliabilitySwitch")
    if (null != reliabilitySwitchValue)
      reliabilitySwitch = reliabilitySwitchValue.toString.toBoolean
  }
}
