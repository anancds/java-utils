package com.cds.utils.hbase;

import com.cds.utils.HdfsOperate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

/**
 * Created by chendongsheng5 on 2017/3/30.
 */
public class DataSourceUtil {

  public final static Log LOG = LogFactory.getLog(DataSourceUtil.class);

  public final static String ROWKEY_COL_NAME = "rowKey";
  public final static String INDICES_KEY_NAME = "indices";
  public final static String COL_KEY_NAME = "col";
  public final static String INDEX_KEY_NAME = "index";
  public final static String OPERATE_KEY_NAME = "operate";
  public final static String OPTIONS_KEY_NAME = "options";
  public final static String SCHEMA_URI = "spark.sql.tableSchema.uri"; // "hdfs://node6:8020"
  //public final static String TABLE_FILE_URI = "spark.sql.tableFile.uri";
  public final static String COMMENT_KEY = "comment";

  public final static String JOBSCHEDULER_ZK = "spark.sql.JobScheduler.zk";
  public final static String JOBSCHEDULER_USER = "spark.sql.JobScheduler.user";
  public final static String JOBSCHEDULER_PASSWORD = "spark.sql.JobScheduler.password";

  public final static String DATA_COLLECTOR_TYPE = "spark.sql.data.collector.type";
  public final static String DATA_COLLECTOR_TYPE_DEFAULT = "com.hikvision.bigdata.ude.datamanipulator.DataCollectorViaKafka";

  public final static String KV_PARAMS_SEPARATOR = ",";
  public final static String KV_PARAM_VALUE_SEPARATOR = ":";

  public final static String SCHEMA_PATH = "spark.sql.tableSchema.path";
  public final static String SCHEMA_PATH_DEFAULT = "/ude/schema/";

  public final static String FILE_PATH = "spark.sql.tableFile.path";
  public final static String FILE_PATH_DEFAULT = "/ude/file/";

  public final static String SCHEMA_ORG_PATH = "spark.sql.tableSchema.org.path";
  public final static String SCHEMA_ORG_PATH_DEFAULT = "/ude/schemaOrg/";

  public final static String SCHEMA_HBP_PATH = "spark.sql.hbpSchema.org.path";
  public final static String SCHEMA_HBP_PATH_DEFAULT = "/ude/hbpSchema/";

  //table path
  public final static String TABLE_PATH = "spark.sql.table.org.path";
  public final static String TABLE_PATH_DEFAULT = "/ude/table/";

  public final static String ZK_Addr = "spark.sql.zk";

  public final static String TABLE_NAME = "tableName";

  public final static String QUERY_LIMIT_ENABLE = "spark.sql.query.limit.enable";
  public final static boolean QUERY_LIMIT_ENABLE_DEFAULT = true;

  public final static String QUERY_LIMIT = "spark.sql.query.limit";
  public final static int QUERY_LIMIT_DEFAULT = 1000;

  public final static String QUERY_COUNT_DISABLE = "spark.sql.query.count.disable";
  public final static boolean QUERY_COUNT_DISABLE_DEFAULT = false;


  //Zookeeper配置参数
  public final static String BASE_SLEEP_TIMRMS = "spark.sql.zk.baseSleepTimeMs";
  public final static int BASE_SLEEP_TIMRMS_DEFAULT = 1000;

  public final static String MAX_RETRIES = "spark.sql.zk.maxRetries";
  public final static int MAX_RETRIES_DEFAULT = 35;

  public final static String CONNECTION_TIMEOUT_MS = "spark.sql.zk.connectionTimeoutMs";
  public final static int CONNECTION_TIMEOUT_MS_DEFAULT = 60000;

  public final static String SESSION_TIMEOUT_MS = "spark.sql.zk.sessionTimeoutMs";
  public final static int SESSION_TIMEOUT_MS_DEFAULT = 10000;

  public final static String ZK_REGISTER_SLEEPTIME = "spark.sql.zk.register.sleepTime";
  public final static int ZK_REGISTER_SLEEPTIME_DEFAULT = 100;

  public final static String PARALLELIZE_NUM_FOR_SOLR = "spark.sql.solr.parallelize.num";
  public final static int PARALLELIZE_NUM_FOR_SOLR_DEFAULT = 1;

  // DataCube Optimization related Configure
  public final static String CUBE_OPTIMIZATION_ENABLE = "spark.sql.cube.optimization.enable";
  public final static boolean CUBE_OPTIMIZATION_ENABLE_DEFAULT = true;

  // Index Optimization related Configure
  public final static String INDEX_OPTIMIZATION_ENABLE = "spark.sql.index.optimization.enable";
  public final static boolean INDEX_OPTIMIZATION_ENABLE_DEFAULT = true;

  public final static String INDEX_OPTIMIZATION_GROUPBY_ENABLE = "spark.sql.index.optimization.groupby.enable";
  public final static boolean INDEX_OPTIMIZATION_GROUPBY_ENABLE_DEFAULT = true;

  // enable multi-shards optimization related Configure
  public final static String INDEX_OPTIMIZATION_MULTISHARDS_ENABLE = "spark.sql.index.multishards.enable";
  public final static boolean INDEX_OPTIMIZATION_MULTISHARDS_ENABLE_DEFAULT = true;

  // enable deep multi-shards optimization related Configure
  public final static String INDEX_OPTIMIZATION_MULTISHARDS_DEEP_ENABLE = "spark.sql.index.multishards.deep.enable";
  public final static boolean INDEX_OPTIMIZATION_MULTISHARDS_DEEP_ENABLE_DEFAULT = false;

  // enable index return total count related Configure
  public final static String INDEX_RETURN_TOTAL_ENABLE = "spark.sql.index.return.total";
  public final static boolean INDEX_RETURN_TOTAL_ENABLE_DEFAULT = true;

  // 唯一sessionID属性标示
  public final static String  SPARK_JOB_GROUP_ID = "spark.jobGroup.id";
  public final static String  INDEX_NEED_TOTAL = "isNeedTotal";

  // cache相关
  // 查询ID保存有效期(最后一次使用期满释放时间)单位为秒，缺省为300秒，缓存池能容纳的最大句柄数，缺省为20个。
  public final static String QUERY_CACHE_EXPIRE = "spark.sql.query.cache.expireAfterAccess";
  public final static String QUERY_CACHE_EXPIRE_DEFAULT = "300";
  public final static String QUERY_CACHE_MAXIMUM = "spark.sql.query.cache.maximumSize";
  public final static String QUERY_CACHE_MAXIMUM_DEFAULT = "20";

  //缓存去掉offset、limit的sql的hash值
  public final static String SPARK_SQL_EXP = "spark.sql.statement";

  //conf for CartesianProducts,if
//    public final static String SPARK_SQL_CROSSJOIN_ENABLE = "spark.sql.crossJoin.enabled";
//    public final static boolean SPARK_SQL_CROSSJOIN_ENABLE_DEFAULT = false;
  public static Tuple2<scala.collection.immutable.Map<String, Tuple2<String, String>>,
      scala.collection.immutable.Map<String, String>>
  readSchemaInfo(String fileName, String basePath)
      throws Exception {

    byte[] value = HdfsOperate.readFullFile(fileName, basePath);
    String json = Bytes.toString(value);
    Map map = jsonStringToMap(json);

    Tuple2<scala.collection.immutable.Map<String, Tuple2<String, String>>,
        scala.collection.immutable.Map<String, String>> tuple2 = readColumnInfo(map, fileName);
    // 读取列模式信息
    scala.collection.immutable.Map<String, Tuple2<String, String>> colMap =
        tuple2._1();

    // 读取列描述信息
    scala.collection.immutable.Map<String, String> commentMap = tuple2._2();

    return new Tuple2<scala.collection.immutable.Map<String, Tuple2<String, String>>,
        scala.collection.immutable.Map<String, String>>(colMap, commentMap);
  }

  public static Tuple2<scala.collection.immutable.Map<String, Tuple2<String, String>>,
      scala.collection.immutable.Map<String, String>>
  readColumnInfo(
      Map map, String fileName) throws Exception {

    Map colInfo = (Map) map.get(COL_KEY_NAME);
    if (null == colInfo) {
      throw new Exception("The column info of " + fileName + " is none.");
    }
    Iterator iter = colInfo.entrySet().iterator();
    Map.Entry next;
    scala.collection.immutable.Map<String, Tuple2<String, String>> colMap =
        new scala.collection.immutable.HashMap<String, Tuple2<String, String>>();
    scala.collection.immutable.Map<String, String> commentMap =
        new scala.collection.immutable.HashMap<String, String>();
    while (iter.hasNext()) {
      next = (Map.Entry) iter.next();
      if (next.getValue() instanceof Map) {
        Map tempMap = (Map) next.getValue();
        String comment = (String) tempMap.remove(COMMENT_KEY);
        if (null != comment) {
          commentMap = commentMap.updated((String) next.getKey(), comment);
        }
        String tmepKey = (String) tempMap.keySet().iterator().next();
        colMap = colMap.updated((String) next.getKey(),
            new Tuple2<String, String>((String) tempMap.get(tmepKey), tmepKey));
      } else {
        colMap = colMap.updated((String) next.getKey(),
            new Tuple2<String, String>((String) next.getValue(), null));
      }
    }
    return new Tuple2<scala.collection.immutable.Map<String, Tuple2<String, String>>,
        scala.collection.immutable.Map<String, String>>(colMap, commentMap);
  }

  public static Map jsonStringToMap(String jsonString) throws JSONException {

    JSONObject jsonObject = new JSONObject(jsonString);
    Map<String, Object> result = new HashMap<String, Object>();
    Iterator iterator = jsonObject.keys();
    String key;

    while (iterator.hasNext()) {
      key = (String) iterator.next();
      if (jsonObject.get(key) instanceof JSONObject) {
        Map value = jsonStringToMap(jsonObject.getString(key));
        result.put(key, value);
      } else {
        String value = jsonObject.getString(key);
        result.put(key, value);
      }
    }
    return result;
  }
}
