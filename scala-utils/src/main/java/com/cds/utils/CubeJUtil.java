package com.cds.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cds.utils.common.CubeNode;
import com.cds.utils.common.CubeTree;
import com.cds.utils.mgr.CubeNodeStatus;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * Created by chendongsheng5 on 2017/3/24.
 */
public class CubeJUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CubeJUtil.class);

  public static String stringFormat(String format, Integer value) {
    return String.format(format, value);
  }


  public static Boolean checkValidDate(String date_str, String dataType) {
    try {
      SimpleDateFormat format;
      if (dataType.contains("Date")) {
        format = new SimpleDateFormat("yyyy-MM-dd");
      } else {
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      }
      format.setLenient(false);
      format.parse(date_str);
      return true;
    } catch (Exception ex) {
      return false;
    }
  }

  /**
   * cube tree to json and load to hdfs
   *
   * @param cubeTree CubeTree
   */
  public static void treeToJson(CubeTree cubeTree, String hdfsBasePath, String cubeName,
      String tableName, String newPath) throws Exception {
    String minLevel = "\"minLevel\":\"" + cubeTree.MIN_LEVEL() + "\"";
    String dateSegment =
        "\"dateSegment\":\"" + cubeTree.DATE_SEGMENT()._1() + "," + cubeTree.DATE_SEGMENT()._2()
            + "\"";
    String datePrefix = "\"datePrefix\":\"" + cubeTree.datePrefix() + "\"";
    String format = "\"format\":\"" + cubeTree.format() + "\"";
    String timeFormatSeq = "\"timeFormatSeq\":" + JSON
        .toJSONString(JavaConversions.asJavaCollection(cubeTree.timeFormatSeq()));
    String timeZone = "\"timeZone\":\"" + cubeTree.timeZone() + "\"";

    StringBuilder tree = new StringBuilder("\"cube_tree\":{");
    if (!JavaConversions.mapAsJavaMap(cubeTree.CUBE_TREE()).isEmpty()) {
      for (java.util.Map.Entry<String, CubeNode> entry : JavaConversions
          .mapAsJavaMap(cubeTree.CUBE_TREE()).entrySet()) {
        tree.append("\"" + entry.getKey() + "\":" + entry.getValue().toString() + ",");
      }
      tree.deleteCharAt(tree.lastIndexOf(","));
    }
    tree.append("}");
    String dictionary =
        "\"yearId\":" + JSON.toJSON(JavaConversions.mapAsJavaMap(cubeTree.YEAR_DICTIONARY()));

    String json = "{" + minLevel + "," + datePrefix + "," + dateSegment + ","
        + format + "," + timeFormatSeq + "," + timeZone + "," + tree + "," + dictionary + "}";

    //load to hdfs
    HdfsOperate.writeFile(newPath + tableName + "/" + cubeName, "cube_tree_info_copy",
        json.getBytes("utf-8"), hdfsBasePath);
    HdfsOperate
        .writeFile(newPath + tableName + "/" + cubeName, "cube_tree_info", json.getBytes("utf-8"),
            hdfsBasePath);
  }

  /**
   * json to cube tree
   *
   * @param json json string
   * @return CubeTree
   */
  public static CubeTree jsonToTree(String json) throws IOException {
    CubeTree cubeTree = new CubeTree();
    JSONObject jsonObject = JSON.parseObject(json);
    //todo recover cube tree
    String minLevel = jsonObject.getString("minLevel");
    cubeTree.MIN_LEVEL_$eq(minLevel);

    String[] segment = jsonObject.getString("dateSegment").split(",");
    cubeTree.DATE_SEGMENT_$eq(new Tuple2<String, String>(segment[0], segment[1]));

    String datePrefix = jsonObject.getString("datePrefix");
    if (datePrefix.equals("null")) {
      datePrefix = null;
    }
    cubeTree.datePrefix_$eq(datePrefix);

    String format = jsonObject.getString("format");
    cubeTree.format_$eq(format);

    String timeZone = jsonObject.getString("timeZone");
    cubeTree.timeZone_$eq(timeZone);

    JSONArray timeSeqObject = jsonObject.getJSONArray("timeFormatSeq");
    for (Object timeSeq : timeSeqObject) {
      cubeTree.addTimeSeq(timeSeq.toString());
    }

    JSONObject treeObject = jsonObject.getJSONObject("cube_tree");
    for (java.util.Map.Entry<String, Object> entry : treeObject.entrySet()) {
      String nodeName = entry.getKey();
      CubeNode cubeNode = new CubeNode(nodeName);
      JSONObject nodeObject = JSON.parseObject(entry.getValue().toString());
      cubeNode.setStart(nodeObject.get("start").toString());
      cubeNode.setEnd(nodeObject.get("end").toString());
      cubeNode
          .setStatus(CubeNodeStatus.valueOf(Integer.valueOf(nodeObject.get("status").toString())));
      cubeNode.setTier(nodeObject.get("tier").toString());
      cubeNode.setReliability(Integer.valueOf(nodeObject.get("reliability").toString()));
      cubeTree.addNodeViaName(nodeName, cubeNode);
    }

    JSONObject yearObject = jsonObject.getJSONObject("yearId");
    for (java.util.Map.Entry<String, Object> entry : yearObject.entrySet()) {
      cubeTree.addYearID(entry.getKey(), entry.getValue().toString());
    }
    return cubeTree;
  }

  public static void paramsToJson(Map<String, String> params, String tableName, String hdfsBasePath,
      String newPath) throws Exception {
    String json = JSON.toJSON(params).toString();
    HdfsOperate
        .writeFile(newPath + tableName, "table_cubes_info", json.getBytes("utf-8"), hdfsBasePath);
  }

  public static Map<String, String> jsonToParams(String tableCubesInfo) {
    Map<String, String> params = new HashMap<>();
    JSONObject Object = JSON.parseObject(tableCubesInfo);
    for (java.util.Map.Entry<String, Object> entry : Object.entrySet()) {
      params.put(entry.getKey(), entry.getValue().toString());
    }
    return params;
  }

  public static Boolean matchDate(String name, List<String> list) {
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i).contains(name)) {
        return true;
      }
    }
    return false;
  }
}
