package com.cds.utils.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by chendongsheng5 on 2017/3/27.
 */
public class HbaseTableManager {

  private static Connection connection;
  private static Admin admin;

  public static void createTable(String tableName, String zk,
      Map<String, String> dictionaryDimension, List<String> dimensionCol) throws Exception {
    try {
      Configuration conf = new Configuration();
      conf.set("hbase.zookeeper.quorum", zk);
      try {
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
      } catch (MasterNotRunningException e) {
        e.printStackTrace();
      } catch (ZooKeeperConnectionException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      if (!admin.tableExists(TableName.valueOf(tableName))) {

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        hcd.setBlockCacheEnabled(true).setBlocksize(1048576).setInMemory(true).setMaxVersions(1)
            .setMinVersions(1);
        htd.addFamily(hcd);

      } else {
        throw new Exception("hbase table " + tableName + " already exist!");
      }
    } catch (IOException e) {
      throw e;
    } finally {
      try {
        if (admin != null) {
          admin.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void dropTable(String zk, String hbaseTableName) throws Exception {

    try {
      Configuration conf = new Configuration();
      conf.set("hbase.zookeeper.quorum", zk);
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      if (!admin.tableExists(TableName.valueOf(hbaseTableName))) {
        throw new IOException("The hbase table " + hbaseTableName + " does not exist.");
      }
      admin.disableTable(TableName.valueOf(hbaseTableName));
      admin.deleteTable(TableName.valueOf(hbaseTableName));
    } finally {
      try {
        if (null != admin) {
          admin.close();
        }
        if (null != connection) {
          connection.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static boolean isTableExist(String zk, String hbaseTableName) throws Exception {

    try {
      Configuration conf = new Configuration();
      conf.set("hbase.zookeeper.quorum", zk);
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      return admin.tableExists(TableName.valueOf(hbaseTableName));
    } finally {
      try {
        if (null != admin) {
          admin.close();
        }
        if (null != connection) {
          connection.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void createCubeTable(String cubeTableName,
      String zk,
      List<String> dimensionsID,
      Map<String, String> years) throws IOException {
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", zk);
    try {
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      TableName tableName = TableName.valueOf(cubeTableName);
      if (!admin.tableExists(tableName)) {
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        hcd.setBlockCacheEnabled(true).setBlocksize(1048576).setInMemory(true).setMaxVersions(1)
            .setMinVersions(1);
        htd.addFamily(hcd);

        byte[][] splitKeys = new byte[dimensionsID.size() * years.size()][];
        int i = 0;
        for (Map.Entry<String, String> year : years.entrySet()) {
          String mark = year.getValue();
          for (String dim : dimensionsID) {
//            splitKeys[i] = Bytes.toBytes(mark + CubeBaseInfo.ROWKEY_SEPARATOR + dim);
            i++;
          }
        }
        admin.createTable(htd, splitKeys);
      } else {
        throw new IOException(" HBase table " + tableName + " already exist. ");
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  public static void dropCubeTable(String cubeTableName, Configuration conf) throws IOException {
    try {
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      TableName tableName = TableName.valueOf(cubeTableName);
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } else {
        throw new IOException(" HBase table " + tableName + " does not exist. ");
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  public static boolean isCubeTableExist(String tableName, Configuration conf) throws IOException {
    try {
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      return admin.tableExists(TableName.valueOf(tableName));
    } finally {
      if (admin != null) {
        admin.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    //"date" -> "00", "catagory" -> "01", "tclass" -> "02", "name" -> "03"
    Map<String, String> dictionaryDimension = new HashMap<String, String>();
    dictionaryDimension.put("date", "00");
    dictionaryDimension.put("catagory", "01");
    dictionaryDimension.put("tclass", "02");
    dictionaryDimension.put("name", "03");

    //"date", "catagory", "tclass", "name"
    List<String> dimensionCol = new ArrayList<>();
    dimensionCol.add("date");
    dimensionCol.add("catagory");
    dimensionCol.add("tclass");
    dimensionCol.add("name");
    createTable("cubeTest", "hdh230,hdh231,hdh232", dictionaryDimension, dimensionCol);
  }
}
