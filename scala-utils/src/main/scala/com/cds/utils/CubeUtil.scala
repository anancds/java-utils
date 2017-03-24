package com.cds.utils

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.sources._

import scala.collection.mutable.{ArrayBuffer, Map}

/**
  * Created by chendongsheng5 on 2017/3/24.
  */
object CubeUtil {

  def getValueFromEqualTo(equal: EqualTo): Any = {

    equal match {
      case EqualTo(attribute: String, value: Any) =>
        value
    }
  }

  /**
    * In order to make startRow exclusive
    * or
    * In order to make stopRow inclusive
    */
  def addTrailing0Byte(row: Array[Byte]): Array[Byte] = {

    val tempRow = new Array[Byte](row.length + 1)
    System.arraycopy(row, 0, tempRow, 0, row.length)
    tempRow(row.length) = 0
    tempRow
  }

  /**
    * 为最后一个字节做+1操作
    * 在hbase中，通过对startRow做该操作生成endRow，可以获取符合某前缀的全部记录
    * 例如startRow="abc"，通过该操作后生成endRow="abd"，则可获取所有前缀为"abc"的记录
    *
    */
  def plus1ForLastByte(srcRow: Array[Byte]): Array[Byte] = {

    val tempRow = new Array[Byte](srcRow.length)
    System.arraycopy(srcRow, 0, tempRow, 0, srcRow.length)
    val endBit = (srcRow(srcRow.length - 1) + 1).toByte
    tempRow(srcRow.length - 1) = endBit
    tempRow
  }

  /**
    * @param sortedIdSet : 已排序的维度ID集合
    * @return 生成的仅包含维度ID的RowKey前缀,String类型
    */
  def genRowKeyIDPrefix(sortedIdSet: Seq[String]): String = {

    val idPart: StringBuilder = StringBuilder.newBuilder
    idPart.append(CubeBaseInfo.getDimSpaceID(sortedIdSet.size)).append(CubeBaseInfo.ROWKEY_SEPARATOR)
    sortedIdSet.foreach((id) => {
      idPart.append(id)
    })

    idPart.toString()
  }

  /**
    * @param tuple : 按维度ID的Map<id, value>
    * @return 生成的RowKey
    */
  def genSpecificRowKey(tuple: (String, Map[String, String]), cubeVaildType: String, fitDimensionId: Seq[String]): Array[Byte] = {

    Bytes.toBytes(genSpecificRowKeyStr(tuple, cubeVaildType, fitDimensionId))
  }

  /**
    * @param tuple : 按维度ID的Map<id, value>
    * @return 生成的RowKey
    */
  def genSpecificRowKeyString(tuple: (String, Map[String, String]), cubeVaildType: String, fitDimensionId: Seq[String]): String = {

    genSpecificRowKeyStr(tuple, cubeVaildType, fitDimensionId)
  }

  /**
    * @param tuple : <yearId,按维度ID的Map<id, value>>
    * @return 生成的RowKey,String类型
    */
  def genSpecificRowKeyStr(tuple: (String, Map[String, String]), cubeVaildType: String, fitDimensionId: Seq[String]): String = {
    val idPart: StringBuilder = StringBuilder.newBuilder
    idPart.append(tuple._1).append(CubeBaseInfo.ROWKEY_SEPARATOR).append(CubeBaseInfo.getDimSpaceID(tuple._2.size)).append(CubeBaseInfo.ROWKEY_SEPARATOR)
    val valuePart: StringBuilder = StringBuilder.newBuilder
    fitDimensionId.foreach(id => {
      idPart.append(id)
      val value = tuple._2.get(id).get
      if (null != value) {
        valuePart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(value)
      }
    })

    idPart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(cubeVaildType).append(valuePart).toString()
  }

  /**
    * @param tuple : <yearId,按维度ID的Map<id, value>>
    * @return 生成的RowKey,String类型
    */
  def genRowKeyStr4Group(tuple: (String, Map[String, String]), cubeVaildType: String, groupDimId: Seq[String], fitDimensionId: Seq[String], isFixGroup: Boolean, isNeedWild: Boolean): String = {

    val idPart: StringBuilder = StringBuilder.newBuilder
    idPart.append(tuple._1).append(CubeBaseInfo.ROWKEY_SEPARATOR).append(CubeBaseInfo.getDimSpaceID(fitDimensionId.size + groupDimId.size)).append(CubeBaseInfo.ROWKEY_SEPARATOR)
    val valuePart: StringBuilder = StringBuilder.newBuilder
    fitDimensionId.foreach(id => {
      idPart.append(id)
      val value = tuple._2.get(id).get
      if (null != value) {
        valuePart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(value)
      }
    })
    groupDimId.foreach(id => {
      idPart.append(id)
      if (isFixGroup) {
        if (tuple._2.get(id) != None) {
          valuePart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(tuple._2.get(id).get)
        } else {
          if (isNeedWild) {
            valuePart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(CubeBaseInfo.MIDDLE_MATCHING_REGEX)
          }
        }
      }
    })
    idPart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(cubeVaildType).append(valuePart).toString()
  }

  /**
    * 时间报表层级生成rowKey
    *
    * @param tuple
    * @param cubeVaildType
    * @param groupDimId
    * @param fitDimensionId
    * @return
    */
  def genRowKeyStr4HierarchyGroup(tuple: (String, Map[String, String]), cubeVaildType: String, groupDimId: Seq[String],
                                  fitDimensionId: Seq[String], isFixGroup: Boolean, isNeedWild: Boolean): String = {

    val idPart: StringBuilder = StringBuilder.newBuilder
    idPart.append(tuple._1).append(CubeBaseInfo.ROWKEY_SEPARATOR).append(CubeBaseInfo.getDimSpaceID(fitDimensionId.size + groupDimId.size)).append(CubeBaseInfo.ROWKEY_SEPARATOR)
    val valuePart: StringBuilder = StringBuilder.newBuilder
    fitDimensionId.foreach(id => {
      idPart.append(id)
      val value = tuple._2.get(id).get
      if (null != value) {
        valuePart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(value)
      }
    })
    groupDimId.foreach(id => {
      idPart.append(id)
      if (isFixGroup) {
        if (tuple._2.get(id) != None) {
          valuePart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(tuple._2.get(id).get)
        } else {
          if (isNeedWild) {
            valuePart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(CubeBaseInfo.MIDDLE_MATCHING_REGEX)
          }
        }
      }
    })
    idPart.append(CubeBaseInfo.ROWKEY_SEPARATOR).append(cubeVaildType).append(valuePart).toString()
  }

  def genRowKeyStr4GroupSet(seq: Seq[(String, Map[String, String])], cubeVaildType: String, groupDimId: Seq[String], fitDimensionId: Seq[String], isFixGroup: Boolean = false, isNeedWild: Boolean = false): ArrayBuffer[String] = {

    val rowKeySet = ArrayBuffer[String]()
    seq.foreach((tupleSortedMap) => {
      //TODO：需要修改
      rowKeySet += genRowKeyStr4Group(tupleSortedMap, cubeVaildType, groupDimId, fitDimensionId, isFixGroup, isNeedWild)
    })
    rowKeySet
  }

  def genRowKeyStr4HierarchyGroupSet(seq: Seq[(String, Map[String, String])], cubeVaildType: String, groupDimId: Seq[String], fitDimensionId: Seq[String], isFixGroup: Boolean = false, isNeedWild: Boolean = false): ArrayBuffer[String] = {

    val rowKeySet = ArrayBuffer[String]()
    seq.foreach((tupleSortedMap) => {
      //TODO：需要修改
      rowKeySet += genRowKeyStr4HierarchyGroup(tupleSortedMap, cubeVaildType, groupDimId, fitDimensionId, isFixGroup, isNeedWild)
    })
    rowKeySet
  }

  /**
    * @param seq : 按维度ID排序的Map<id, value>组成的列表
    * @return 生成的RowKey集合
    */
  def genSpecificRowKeySet(seq: Seq[(String, Map[String, String])], cubeVaildType: String, fitDimensionId: Seq[String]): ArrayBuffer[Array[Byte]] = {

    val rowKeySet = ArrayBuffer[Array[Byte]]()
    seq.foreach((tupleSortedMap) => {
      //TODO：需要修改
      rowKeySet += genSpecificRowKey(tupleSortedMap, cubeVaildType, fitDimensionId)
    })

    rowKeySet
  }

  /**
    * @param seq : 按维度ID排序的Map<id, value>组成的列表
    * @return 生成的RowKey集合
    */
  def genSpecificRowKeyStringSet(seq: Seq[(String, Map[String, String])], cubeVaildType: String, fitDimensionId: Seq[String]): ArrayBuffer[String] = {

    val rowKeySet = ArrayBuffer[String]()
    seq.foreach((tupleSortedMap) => {
      //TODO：需要修改
      rowKeySet += genSpecificRowKeyString(tupleSortedMap, cubeVaildType, fitDimensionId)
    })

    rowKeySet
  }

  /**
    * @param seq : 按维度ID排序的Map<id, value>组成的列表
    * @return 生成的RowKey集合,String类型
    */
  def genSpecificRowKeyStrSet(seq: Seq[Map[String, String]]): ArrayBuffer[String] = {

    val rowKeySet = ArrayBuffer[String]()
    seq.foreach((sortedMap) => {
      //TODO:需要修改
      //      rowKeySet += genSpecificRowKeyStr(sortedMap)
    })

    rowKeySet
  }

  def genHBaseColForAggr(aggregateSet: Seq[NamedExpression]): scala.collection.mutable.Map[String, (String, String)] = {

    val hbaseColsMap = scala.collection.mutable.Map[String, (String, String)]()
    aggregateSet.map {
      case e@expressions.Alias(aggregate, name) =>
        aggregate match {
          case e@AggregateExpression(expressions.aggregate.Count(child),_,_,_) =>
            val colName = child.seq(0).references.toSeq(0).name
            val aggrCol = CubeBaseInfo.MEASURE_COUNT_LEFT + colName + CubeBaseInfo.MEASURE_RIGHT
            hbaseColsMap += name ->(CubeBaseInfo.MEASURE_CF, aggrCol)
          /*          case e@expressions.CountDistinct(child) =>
                      val colName = child.seq(0).references.toSeq(0).name
                      val aggrCol = CubeBaseInfo.MEASURE_COUNT_DISTINCT_LEFT + colName + CubeBaseInfo.MEASURE_RIGHT
                      hbaseColsMap += name ->(CubeBaseInfo.MEASURE_CF, aggrCol)*/
          case e@AggregateExpression(expressions.aggregate.Min(child),_,_,_) =>
            val colName = child.references.toSeq(0).name
            val aggrCol = CubeBaseInfo.MEASURE_MIN_LEFT + colName + CubeBaseInfo.MEASURE_RIGHT
            hbaseColsMap += name ->(CubeBaseInfo.MEASURE_CF, aggrCol)
          case e@AggregateExpression(expressions.aggregate.Max(child),_,_,_) =>
            val colName = child.references.toSeq(0).name
            val aggrCol = CubeBaseInfo.MEASURE_MAX_LEFT + colName + CubeBaseInfo.MEASURE_RIGHT
            hbaseColsMap += name ->(CubeBaseInfo.MEASURE_CF, aggrCol)
          case e@AggregateExpression(expressions.aggregate.Sum(child),_,_,_) =>
            val colName = child.references.toSeq(0).name
            val aggrCol = CubeBaseInfo.MEASURE_SUM_LEFT + colName + CubeBaseInfo.MEASURE_RIGHT
            hbaseColsMap += name ->(CubeBaseInfo.MEASURE_CF, aggrCol)
          case e@AggregateExpression(expressions.aggregate.Average(child),_,_,_) =>
            val colName = child.references.toSeq(0).name
            val aggrCol = CubeBaseInfo.MEASURE_AVER_LEFT + colName + CubeBaseInfo.MEASURE_RIGHT
            hbaseColsMap += name ->(CubeBaseInfo.MEASURE_CF, aggrCol)
        }
      case _ =>
    }

    hbaseColsMap
  }

  /**
    * 获取N个集合的笛卡尔积
    * 假如传入为：[[( 1, a1 ), ( 1, a 2 ), ( 1, a 3 ) ], [ ( 2, b 1 ), ( 2, b 2 ) ], [ ( 3, c 1 ), ( 3, c 2 )]]
    * 设：
    * s1 = [(1, a1), (1, a2), (1, a3)]
    * s2 = [(2, b1), (2, b2)]
    * s3 = [(3, c1), (3, c2)]
    * 其大小分别为：s1_length=3，s2_length=2，s3_length=2，
    * 目标list的总大小为：totalSize=3*2*2 = 12
    * 对每个子集s1，s2，s3，进行循环次数=总记录数/(元素个数*后续集合的笛卡尔积个数)
    * 对s1中的每个元素循环次数=总记录数/(元素个数*后续集合的笛卡尔积个数)=12/(3*4)=1次，每个元素每次循环打印次数：后续集合的笛卡尔积个数=2*2个
    * 对s2中的每个元素循环次数=总记录数/(元素个数*后续集合的笛卡尔积个数)=12/(2*2)=3次，每个元素每次循环打印次数：后续集合的笛卡尔积个数=2个
    * 对s3中的每个元素循环次数=总记录数/(元素个数*后续集合的笛卡尔积个数)=12/(2*1)=6次，每个元素每次循环打印次数：后续集合的笛卡尔积个数=1个
    *
    * 运算结果为：
    * [
    * {(1, a1), (2, b1), (3, c1)}
    * {(1, a1), (2, b1), (3, c2)}
    * {(1, a1), (2, b2), (3, c1)}
    * {(1, a1), (2, b2), (3, c2)}
    * {(1, a2), (2, b1), (3, c1)}
    * {(1, a2), (2, b1), (3, c2)}
    * {(1, a2), (2, b2), (3, c1)}
    * {(1, a2), (2, b2), (3, c2)}
    * {(1, a3), (2, b1), (3, c1)}
    * {(1, a3), (2, b1), (3, c2)}
    * {(1, a3), (2, b2), (3, c1)}
    * {(1, a3), (2, b2), (3, c2)}
    * ]
    *
    * @param seq 集合列表，集合本身也是一个列表，集合元素为Tuple1类型即KeyValue形式
    * @return 全部组合列表，单个组合保存到Map
    */
  def genDescartes(seq: Seq[Seq[(String, String)]]): Seq[Map[String, String]] = {

    var total = 1
    seq.foreach((seq) => {
      total = total * seq.size
    })

    val returnSeq = new Array[Map[String, String]](total)
    // 当前笛卡尔积数
    var currentNum = 1
    // 每个元素每次循环输出数
    var outputPerItem = 1
    // 每个元素循环的总次数
    var loopPerItem = 1
    seq.foreach((seq) => {
      currentNum = currentNum * seq.size
      // 目标数组的索引值
      var index = 0
      val currentSize = seq.size
      outputPerItem = total / currentNum
      loopPerItem = total / (outputPerItem * currentSize)
      for (i <- 0 to (loopPerItem - 1)) {
        seq.foreach((tuple) => {
          for (j <- 0 to (outputPerItem - 1)) {
            if (returnSeq(index) == null) {
              returnSeq(index) = Map[String, String](tuple)
            } else {
              returnSeq(index) += tuple
            }
            index = index + 1
          }
        })
      }

    })

    returnSeq
  }

  /**
    * 包括空间id区域与维度id区域
    */
  def getIdFieldFromKey(rowKey: String): String = {

    val idFields = rowKey.split(CubeBaseInfo.ROWKEY_SEPARATOR, 3)
    idFields(0) + CubeBaseInfo.ROWKEY_SEPARATOR + idFields(1)
  }

  def getFieldVarSplit(splitStr: String, sourceStr: String): String = {

    val field = sourceStr.split(splitStr)(0)
    field
  }


  def getDimValueFromKey(rowKey: String, index: Int): String = {

    val list = rowKey.split(CubeBaseInfo.ROWKEY_SEPARATOR)
    // 第二个元素为维度空间id区域，第三个元素为维度id区域，所以index对应的维度值位置为index+1而不是index-1
    val dimID = list(1).toInt
    //其他占用的数据占用4个位置
    if (dimID + 4 != list.length) {
      null
    } else {
      val idFields = list(2)
      val length = idFields.size / CubeBaseInfo.DIMID_DIGITS
      var idDimValueMap = scala.collection.mutable.Map[Int, String]()
      for (i <- 1 to length) {
        val tempID = idFields.substring(i * CubeBaseInfo.DIMID_DIGITS - CubeBaseInfo.DIMID_DIGITS, i * CubeBaseInfo.DIMID_DIGITS)
        idDimValueMap += Integer.parseInt(tempID) -> list(i + 3)
      }

      idDimValueMap.get(index).get
    }
  }

  def rebuildRowKey(orgRowKey: String) = {
    val rowKeyPrefix = orgRowKey.split(CubeBaseInfo.ROWKEY_SEPARATOR)
    val newRowKey = rowKeyPrefix(0) + CubeBaseInfo.ROWKEY_SEPARATOR + rowKeyPrefix(1) + CubeBaseInfo.ROWKEY_SEPARATOR +
      rowKeyPrefix(2) + CubeBaseInfo.ROWKEY_SEPARATOR + rowKeyPrefix(3) + CubeBaseInfo.ROWKEY_SEPARATOR + rowKeyPrefix(4)
    newRowKey
  }

  def getMeasureLeftMark(measureCol: String): String = {

    val measure = measureCol.split("\\" + CubeBaseInfo.MEASURE_LEFT, 2)(0)
    measure + CubeBaseInfo.MEASURE_LEFT
  }

  def measureCombine(measureType: String, valueType: String, m1: Any, m2: Any): Any = {

    measureType match {

      case CubeBaseInfo.MEASURE_COUNT_DISTINCT_LEFT => sumAccordingType(valueType, m1, m2)
      case CubeBaseInfo.MEASURE_COUNT_LEFT => sumAccordingType(valueType, m1, m2)
      case CubeBaseInfo.MEASURE_MAX_LEFT => maxAccordingType(valueType, m1, m2)
      case CubeBaseInfo.MEASURE_MIN_LEFT => minAccordingType(valueType, m1, m2)
      case CubeBaseInfo.MEASURE_SUM_LEFT => sumAccordingType(valueType, m1, m2)
      case CubeBaseInfo.MEASURE_AVER_LEFT => averAccordingType(valueType, m1, m2)
    }
  }

  def sumAccordingType(valueType: String, m1: Any, m2: Any): Any = {

    valueType match {
      case CubeBaseInfo.INT_TYPE => m1.asInstanceOf[Int] + m2.asInstanceOf[Int]
      case CubeBaseInfo.LONG_TYPE => m1.asInstanceOf[Long] + m2.asInstanceOf[Long]
      case CubeBaseInfo.DOUBLE_TYPE => m1.asInstanceOf[Double] + m2.asInstanceOf[Double]
      case CubeBaseInfo.FLOAT_TYPE => m1.asInstanceOf[Float] + m2.asInstanceOf[Float]
      case CubeBaseInfo.SHORT_TYPE => m1.asInstanceOf[Short] + m2.asInstanceOf[Short]
    }
  }

  def minAccordingType(valueType: String, m1: Any, m2: Any): Any = {

    valueType match {
      case CubeBaseInfo.INT_TYPE => if (m1.asInstanceOf[Int] < m2.asInstanceOf[Int]) m1 else m2
      case CubeBaseInfo.LONG_TYPE => if (m1.asInstanceOf[Long] < m2.asInstanceOf[Long]) m1 else m2
      case CubeBaseInfo.DOUBLE_TYPE => if (m1.asInstanceOf[Double] < m2.asInstanceOf[Double]) m1 else m2
      case CubeBaseInfo.FLOAT_TYPE => if (m1.asInstanceOf[Float] < m2.asInstanceOf[Float]) m1 else m2
      case CubeBaseInfo.SHORT_TYPE => if (m1.asInstanceOf[Short] < m2.asInstanceOf[Short]) m1 else m2
    }
  }

  def maxAccordingType(valueType: String, m1: Any, m2: Any): Any = {

    valueType match {
      case CubeBaseInfo.INT_TYPE => if (m1.asInstanceOf[Int] > m2.asInstanceOf[Int]) m1 else m2
      case CubeBaseInfo.LONG_TYPE => if (m1.asInstanceOf[Long] > m2.asInstanceOf[Long]) m1 else m2
      case CubeBaseInfo.DOUBLE_TYPE => if (m1.asInstanceOf[Double] > m2.asInstanceOf[Double]) m1 else m2
      case CubeBaseInfo.FLOAT_TYPE => if (m1.asInstanceOf[Float] > m2.asInstanceOf[Float]) m1 else m2
      case CubeBaseInfo.SHORT_TYPE => if (m1.asInstanceOf[Short] > m2.asInstanceOf[Short]) m1 else m2
    }
  }

  def averAccordingType(valueType: String, m1: Any, m2: Any): Any = ???

  /**
    *
    * @param map1 Map集合1
    * @param map2 Map集合2
    * @return 如果map2所有元素均大于map1所有全部元素则返回true，否则返回false
    */
  def intMapCompare(map1: scala.collection.mutable.Map[String, Int], map2: scala.collection.mutable.Map[String, Int]): Boolean = {

    if (0 == map2.size)
    //      return false
    // 当第二个集合为空时，则认为全部大于
      return true
    intSetCompare(map1.values, map2.values)
  }

  /**
    *
    * @param iter1 集合1迭代器
    * @param iter2 集合2迭代器
    * @return 如果iter2所有元素均大于iter1所有全部元素则返回true，否则返回false
    */
  def intSetCompare(iter1: Iterable[Int], iter2: Iterable[Int]): Boolean = {

    iter1.foreach((srcValue) => {
      iter2.foreach((destValue) => {
        if (srcValue >= destValue) {
          return false
        }
      })
    })

    true
  }

  def checkMeasureFun(funName: String): Boolean = {

    funName match {
      case CubeBaseInfo.MEASURE_COUNT => true
      case CubeBaseInfo.MEASURE_MAX => true
      case CubeBaseInfo.MEASURE_MIN => true
      case CubeBaseInfo.MEASURE_SUM => true
      case _ => false
    }
  }

  def checkMeasureFunWithDataType(funName: String, dataType: String): Boolean = {

    funName match {
      case CubeBaseInfo.MEASURE_COUNT => true
      case CubeBaseInfo.MEASURE_MAX =>
        dataType match {
          case CubeBaseInfo.INT_TYPE => true
          case CubeBaseInfo.LONG_TYPE => true
          case CubeBaseInfo.DOUBLE_TYPE => true
          case CubeBaseInfo.FLOAT_TYPE => true
          case CubeBaseInfo.SHORT_TYPE => true
          case _ => false

        }
      case CubeBaseInfo.MEASURE_MIN =>
        dataType match {
          case CubeBaseInfo.INT_TYPE => true
          case CubeBaseInfo.LONG_TYPE => true
          case CubeBaseInfo.DOUBLE_TYPE => true
          case CubeBaseInfo.FLOAT_TYPE => true
          case CubeBaseInfo.SHORT_TYPE => true
          case _ => false

        }
      case CubeBaseInfo.MEASURE_SUM =>
        dataType match {
          case CubeBaseInfo.INT_TYPE => true
          case CubeBaseInfo.LONG_TYPE => true
          case CubeBaseInfo.DOUBLE_TYPE => true
          case CubeBaseInfo.FLOAT_TYPE => true
          case CubeBaseInfo.SHORT_TYPE => true
          case _ => false

        }
      case _ => false
    }
  }

  //获取时间条件中的年
  def getTimeRangeYear(timeColExpr: Seq[Filter], cubeValidType: String, isTimeScope: Boolean, timeColType: String,
                       yearFormat: String, format: String, timeZone: String) = {
    val map: scala.collection.mutable.Map[String, (String, Boolean)] = new scala.collection.mutable.HashMap[String, (String, Boolean)]()
    if (isTimeScope) {
      timeColExpr.foreach(t => {
        getTimeRange(t, map)
      })
      if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
        val startDate = TimeUnifyUtil.formatLongDate(map.get("Lower").get._1.toLong, format, timeZone)
        val endDate = TimeUnifyUtil.formatLongDate(map.get("Upper").get._1.toLong, format, timeZone)
        TimeUtil.getYear(startDate, endDate, yearFormat, format, timeZone)
      } else {
        TimeUtil.getYear(map.get("Lower").get._1, map.get("Upper").get._1, yearFormat, format, timeZone)
      }
    } else {
      val dates = getTimeDate(timeColExpr)
      dates.flatMap(t => {
        if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
          val date = TimeUnifyUtil.formatLongDate(t.toLong, format, timeZone)
          TimeUtil.getYear(date, date, yearFormat, format, timeZone)
        } else {
          TimeUtil.getYear(t, t, yearFormat, format, timeZone)
        }
      })
    }
  }

  //获取时间条件中的月
  def getTimeRangeMonth(timeColExpr: Seq[Filter], cubeValidType: String, isTimeScope: Boolean, timeColType: String,
                        monthFormat: String, format: String, timeZone: String) = {
    val map: scala.collection.mutable.Map[String, (String, Boolean)] = new scala.collection.mutable.HashMap[String, (String, Boolean)]()
    if (isTimeScope) {
      timeColExpr.foreach(t => {
        getTimeRange(t, map)
      })
      if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
        val startDate = TimeUnifyUtil.formatLongDate(map.get("Lower").get._1.toLong, format, timeZone)
        val endDate = TimeUnifyUtil.formatLongDate(map.get("Upper").get._1.toLong, format, timeZone)
        TimeUtil.getMonth(startDate, endDate, monthFormat, format, timeZone)
      } else {
        TimeUtil.getMonth(map.get("Lower").get._1, map.get("Upper").get._1, monthFormat, format, timeZone)
      }
    } else {
      val dates = getTimeDate(timeColExpr)
      dates.flatMap(t => {
        if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
          val date = TimeUnifyUtil.formatLongDate(t.toLong, format, timeZone)
          TimeUtil.getMonth(date, date, monthFormat, format, timeZone)
        } else {
          TimeUtil.getMonth(t, t, monthFormat, format, timeZone)
        }
      })
    }
  }

  //获取时间条件中的天
  def getTimeRangeDate(timeColExpr: Seq[Filter], cubeValidType: String, isTimeScope: Boolean, timeColType: String,
                       dateFormat: String, format: String, timeZone: String) = {
    val map: scala.collection.mutable.Map[String, (String, Boolean)] = new scala.collection.mutable.HashMap[String, (String, Boolean)]()
    if (isTimeScope) {
      timeColExpr.foreach(t => {
        getTimeRange(t, map)
      })
      if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
        val startDate = TimeUnifyUtil.formatLongDate(map.get("Lower").get._1.toLong, format, timeZone)
        val endDate = TimeUnifyUtil.formatLongDate(map.get("Upper").get._1.toLong, format, timeZone)
        TimeUtil.getDay(startDate, endDate, dateFormat, format, timeZone)
      } else {
        TimeUtil.getDay(map.get("Lower").get._1, map.get("Upper").get._1, dateFormat, format, timeZone)
      }
    } else {
      val dates = getTimeDate(timeColExpr)
      dates.flatMap(t => {
        if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
          val date = TimeUnifyUtil.formatLongDate(t.toLong, format, timeZone)
          TimeUtil.getDay(date, date, dateFormat, format, timeZone)
        } else {
          TimeUtil.getDay(t, t, dateFormat, format, timeZone)
        }
      })
    }
  }

  //获取时间条件中的时
  def getTimeRangeHour(timeColExpr: Seq[Filter], cubeValidType: String, isTimeScope: Boolean, timeColType: String,
                       hourFormat: String, format: String, timeZone: String) = {
    val map: scala.collection.mutable.Map[String, (String, Boolean)] = new scala.collection.mutable.HashMap[String, (String, Boolean)]()
    if (isTimeScope) {
      timeColExpr.foreach(t => {
        getTimeRange(t, map)
      })
      if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
        val startDate = TimeUnifyUtil.formatLongDate(map.get("Lower").get._1.toLong, format, timeZone)
        val endDate = TimeUnifyUtil.formatLongDate(map.get("Upper").get._1.toLong, format, timeZone)
        TimeUtil.getHour(startDate, endDate, hourFormat, format, timeZone)
      } else {
        TimeUtil.getHour(map.get("Lower").get._1, map.get("Upper").get._1, hourFormat, format, timeZone)
      }
    } else {
      val dates = getTimeDate(timeColExpr)
      dates.flatMap(t => {
        if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
          val date = TimeUnifyUtil.formatLongDate(t.toLong, format, timeZone)
          TimeUtil.getHour(date, date, hourFormat, format, timeZone)
        } else {
          TimeUtil.getHour(t, t, hourFormat, format, timeZone)
        }
      })
    }
  }

  //获取时间条件中的分钟
  def getTimeRangeMinute(timeColExpr: Seq[Filter], cubeValidType: String, isTimeScope: Boolean, timeColType: String,
                         minuteFormat: String, format: String, timeZone: String) = {
    val map: scala.collection.mutable.Map[String, (String, Boolean)] = new scala.collection.mutable.HashMap[String, (String, Boolean)]()
    if (isTimeScope) {
      timeColExpr.foreach(t => {
        getTimeRange(t, map)
      })
      if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
        val startDate = TimeUnifyUtil.formatLongDate(map.get("Lower").get._1.toLong, format, timeZone)
        val endDate = TimeUnifyUtil.formatLongDate(map.get("Upper").get._1.toLong, format, timeZone)
        TimeUtil.getMinute(startDate, endDate, minuteFormat, format, timeZone)
      } else {
        TimeUtil.getMinute(map.get("Lower").get._1, map.get("Upper").get._1, minuteFormat, format, timeZone)
      }
    } else {
      val dates = getTimeDate(timeColExpr)
      dates.flatMap(t => {
        if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
          val date = TimeUnifyUtil.formatLongDate(t.toLong, format, timeZone)
          TimeUtil.getMinute(date, date, minuteFormat, format, timeZone)
        } else {
          TimeUtil.getMinute(t, t, minuteFormat, format, timeZone)
        }
      })
    }
  }

  //获取时间条件中的秒
  def getTimeRangeSecond(timeColExpr: Seq[Filter], cubeValidType: String, isTimeScope: Boolean, timeColType: String,
                         secondFormat: String, format: String, timeZone: String) = {
    val map: scala.collection.mutable.Map[String, (String, Boolean)] = new scala.collection.mutable.HashMap[String, (String, Boolean)]()
    if (isTimeScope) {
      timeColExpr.foreach(t => {
        getTimeRange(t, map)
      })
      if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
        val startDate = TimeUnifyUtil.formatLongDate(map.get("Lower").get._1.toLong, format, timeZone)
        val endDate = TimeUnifyUtil.formatLongDate(map.get("Upper").get._1.toLong, format, timeZone)
        TimeUtil.getSecond(startDate, endDate, secondFormat, format, timeZone)
      } else {
        TimeUtil.getSecond(map.get("Lower").get._1, map.get("Upper").get._1, secondFormat, format, timeZone)
      }
    } else {
      val dates = getTimeDate(timeColExpr)
      dates.flatMap(t => {
        if (timeColType.equals("Long") || timeColType.equals("Timestamp")) {
          val date = TimeUnifyUtil.formatLongDate(t.toLong, format, timeZone)
          TimeUtil.getSecond(date, date, secondFormat, format, timeZone)
        } else {
          TimeUtil.getSecond(t, t, secondFormat, format, timeZone)
        }
      })
    }
  }

  //获取时间范围 TODO:先处理Long类型 后续结合其他模块处理date类型的时间
  def getTimeRange(timeColExpr: Filter, map: scala.collection.mutable.Map[String, (String, Boolean)]) = {
    timeColExpr match {
      case LessThan(attribute: String, value: Any) =>
        map.+=(("Upper", (value.toString, false)))
      case LessThanOrEqual(attribute: String, value: Any) =>
        map.+=(("Upper", (value.toString, true)))
      case GreaterThan(attribute: String, value: Any) =>
        map.+=(("Lower", (value.toString, false)))
      case GreaterThanOrEqual(attribute: String, value: Any) =>
        map.+=(("Lower", (value.toString, true)))
    }
  }

  //获取时间范围 TODO:先处理Long类型 后续结合其他模块处理date类型的时间
  def getTimeDate(timeColExprs: Seq[Filter]) = {
    timeColExprs.map(timeColExpr => {
      timeColExpr match {
        case EqualTo(attribute: String, value: Any) =>
          value.toString
      }
    })
  }

  //groupby时需要将同一个yearId的rowKey弄在一起scan
  def separateRowKeyWithYearId(array: ArrayBuffer[String]) = {
    val map = scala.collection.mutable.HashMap[String, scala.collection.mutable.SortedSet[String]]()
    array.foreach(t => {
      val yearId = t.split(CubeBaseInfo.ROWKEY_SEPARATOR)(0)
      if (map.contains(yearId)) {
        map.get(yearId).get.+=(t)
      } else {
        val set = scala.collection.mutable.SortedSet[String]()
        set.+=(t)
        map.+=((yearId, set))
      }
    })
    map
  }

  def compareFilterGroupCol(filter: Seq[String], group: Seq[String]) = {
    val seq = filter.filter(t => !group.contains(t))
    val isContains = seq.size != filter.size
    val istTotalContains = seq.size == (filter.size - group.size)
    (isContains, istTotalContains, seq)
  }

  //不够两位前面补0
  def formatNum(i: Int): String = {
    String.valueOf(i).length match {
      case 1 => "0" + String.valueOf(i)
      case _ => String.valueOf(i)
    }
  }

  def getPrefix(name: String): String = name.substring(0, name.indexOf("("))

  /**
    * 将Seq序列转化成展示的string，用在show cube
    */
  def showSeq(list: Seq[String], separator: String): String = {
    var result: String = ""
    for (value <- list) {
      result += value + separator
    }
    if (!result.equals(""))
      result.substring(0, result.length - separator.length)
    else
      result
  }

  //得到sum(aaa)中括号里的值
  def getAggValue(test: String): String = {
    test.substring(test.indexOf("(") + 1, test.indexOf(")"))
  }

  //替换rowKey中的时间
  def replaceRowKeyDate(rowKey: String, dateIndex: Int): String = {
    val separator = CubeBaseInfo.ROWKEY_SEPARATOR
    val arrayValue = rowKey.split(separator)
    val lowDate = rowKey.split(separator)(dateIndex)
    //每次截取date后三位来作为高位的date
    val highDate = lowDate.substring(0, lowDate.length - 3)
    arrayValue(dateIndex) = highDate
    arrayValue.reduce((t1, t2) => t1 + separator + t2)
  }

  def getRowKeyRegex(rowKeyPrefix: String, date: String, dateIndex: Int, dimLength: Int): String = {
    val separator = CubeBaseInfo.ROWKEY_SEPARATOR
    var result: String = ""
    var flag = true
    var i = 0
    while (flag) {
      if (i == dateIndex) {
        result = result + separator + date
        flag = false
      }
      else
        result = result + separator + "[^" + separator + "]*"
      i = i + 1
    }
    "^" + rowKeyPrefix + result + ".*$"
  }
}
