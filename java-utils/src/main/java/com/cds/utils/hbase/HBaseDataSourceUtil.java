package com.cds.utils.hbase;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;

/**
 * Created by chendongsheng5 on 2017/3/30.
 */
public class HBaseDataSourceUtil extends DataSourceUtil {

  public final static Log LOG = LogFactory.getLog(HBaseDataSourceUtil.class);

  public final static String HBASE_SCAN_CACHE_SIZE = "spark.sql.hbase.scan.cache.size";
  public final static int HBASE_SCAN_CACHE_SIZE_DEFAULT = 1000;

  public final static String INDEX_HBASE_SCAN_CACHE_SIZE = "spark.sql.index.hbase.scan.cache.size";
  public final static int INDEX_HBASE_SCAN_CACHE_SIZE_DEFAULT = 1000;

  /** 当select的列在一个列族中比率达到HBASE_SCAN_ALL_COLUMN_RATIO时scan设置拉取整个列族，
   * 以减少定位列的时间提高性能 **/
  public final static String HBASE_SCAN_ALL_COLUMN_RATIO = "spark.sql.hbase.scan.all.column.ratio";
  public final static double HBASE_SCAN_ALL_COLUMN_RATIO_DEFAULT = 0.65;

  public final static String HBASE_SCANNER_TIMEOUT = "spark.sql.hbase.scanner.timeout";
  public final static String HBASE_SCANNER_TIMEOUT_DEFAULT = "600000"; // ms

  public final static String INDEX_HBASE_SCANNER_TIMEOUT = "spark.sql.index.hbase.scanner.timeout";
  public final static String INDEX_HBASE_SCANNER_TIMEOUT_DEFAULT = "1200000"; // ms

  public final static String HBASE_RPC_TIMEOUT = "spark.sql.hbase.rpc.timeout";
  public final static String HBASE_RPC_TIMEOUT_DEFAULT = "60000"; // ms

  public final static String INDEX_HBASE_RPC_TIMEOUT = "spark.sql.index.hbase.rpc.timeout";
  public final static String INDEX_HBASE_RPC_TIMEOUT_DEFAULT = "120000"; // ms

  public final static String HBASE_RETRIES_NUMBER = "spark.sql.hbase.retries.number";
  public final static int HBASE_RETRIES_NUMBER_DEFAULT = 3;

  public final static String INDEX_ZK_CLIENT_TIMEOUT = "spark.sql.indexer.zk.client.timeout";
  public final static int INDEX_ZK_CLIENT_TIMEOUT_DEFAULT = 10000;
  public final static String INDEX_ZK_CONNECT_TIMEOUT = "spark.sql.indexer.zk.connect.timeout";
  public final static int INDEX_ZK_CONNECT_TIMEOUT_DEFAULT = 10000;

  public final static String ES_CLIENT_PING_TIMEOUT = "spark.sql.es.client.ping.timeout";
  public final static int ES_CLIENT_PING_TIMEOUT_DEFAULT = 180;
  public final static String ES_CLIENT_SNIFF = "spark.sql.es.client.sniff";
  public final static boolean ES_CLIENT_SNIFF_DEFAULT = true;

  public final static String ZK_RECOVERY_RETRY = "spark.sql.zk.recovery.retry";
  public final static int ZK_RECOVERY_RETRY_DEFAULT = 3;

  //深度分页使用的配置项
  public final static String SCAN_HBASE_OFFST="spark.sql.scan.hbase.offset";
  public final static int SCAN_HBASE_OFFST_DEFAULT=1000;

  //public final static String SCAN_INDEX_OFFST="spark.sql.scan.index.offset";
  //public final static int SCAN_INDEX_OFFST_DEFAULT=10000;

  //public final static String DEEP_INDEX_OFFST="spark.sql.deep.index.offset";
  //public final static int DEEP_INDEX_OFFST_DEFAULT=10000;

  public final static String MUTI_INDEX_SCAN_NUM="spark.sql.scan.index.initial.num";
  public final static int MUTI_INDEX_SCAN_NUM_DEFAULT=10;

  public final static String MUTI_INDEX_SCAN_MAX_NUM="spark.sql.scan.index.max.num";
  public final static int MUTI_INDEX_SCAN_MAX_NUM_DEFAULT=50;

  // public final static String DEEP_INDEX_NEED_TOTAL="spark.sql.deep.index.needTotal";
  // public final static boolean DEEP_INDEX_NEED_TOTAL_DEFAULT=true;


  public final static String ZK = "zk"; // HBase集群ZooKeeper地址
  public final static String HBASE_TABLE_NAME = "hbaseTableName";
  public final static String INDEX_URL = "indexerURL";
  public final static String INDEX_INSTANCE = "indexerClass";
  public final static String INCR_INDEXING_ENABLE = "incrIndexingEnable";
  public final static String REGION_COUNT = "regionCount";
  public final static String SPLIT_KEYS = "splitKeys";
  public final static String DATA_BLOCK_ENCODING = "dataBlockEncoding";
  public final static String BLOOM_FILTER_TYPE = "bloomFilterType";
  public final static String COMPRESSION_TYPE = "compressionType";

  //定时region拆分配置项
  public final static String SPLIT_REGION_TIMER = "splitRegionTimer";
  public final static String SPLIT_REGION_TYPE = "splitRegionType";
  public final static String SPLIT_REGION_TIMER_EXPR_DEFAULT = "0 0 23 * * ?";

  public final static String HDFS_DIRECTORY_FLAG = "hdfs:";
  public final static String ROWKEY_SEPARATOR = "-";

  public final static String INDEX_MAX_RETURN_ROW = "spark.sql.index.max.return.row";
  public final static int INDEX_MAX_RETURN_ROW_DEFAULT = 1000;
  public final static String INDEX_TOLERANT = "spark.sql.index.shards.tolerant";
  public final static boolean INDEX_TOLERANT_DEFAULT = true;
  public final static String INDEX_JUTE_MAXBUFFER = "spark.sql.index.shards.tolerant";
  public final static int INDEX_JUTE_MAXBUFFER_DEFAULT = 20496000;
//    public final static int INDEX_MAX_RETURN_ROW_DEFAULT = Integer.MAX_VALUE;

  // 索引zk地址方式格式：zk:hdh137,hdh138,hdh140:testTable
  public final static String INDEX_URL_ZK_FORMAT = "(?i:zk):(.+):(.+)";
  // 索引http地址方式格式：http://hdh138:8984/solr/testTable
  public final static String INDEX_URL_HTTP_PREFIX = "http://";

  /**
   * Parses a combined family and qualifier and adds either both or just the family in case there is no qualifier. This assumes the older colon
   * divided notation, e.g. "family:qualifier".
   *
   * @param scan               The Scan to update.
   * @param familyAndQualifier family and qualifier
   * @throws IllegalArgumentException When familyAndQualifier is invalid.
   */
  public static void addColumn(Scan scan, byte[] familyAndQualifier) {
    byte[][] fq = KeyValue.parseColumn(familyAndQualifier);
    if (fq.length == 1) {
      scan.addFamily(fq[0]);
    } else if (fq.length == 2) {
      scan.addColumn(fq[0], fq[1]);
    } else {
      throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
    }
  }

  /**
   * Writes the given scan into a Base64 encoded string.
   *
   * @param scan The scan to write out.
   * @return The scan saved in a Base64 encoded string.
   * @throws java.io.IOException When writing the scan fails.
   */
  public static String convertScanToString(Scan scan) throws IOException {
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    return Base64.encodeBytes(proto.toByteArray());
  }
}
