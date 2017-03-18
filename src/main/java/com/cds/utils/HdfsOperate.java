package com.cds.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Created by chendongsheng5 on 2017/3/18.
 */
public class HdfsOperate {
  static final Configuration conf = new Configuration();
  static {
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
  }
//    public static String basePath = "hdfs://hdh140:8020";

  public static void writeFile(String root, String currentPath, byte[] data, String basePath) throws Exception {
    FileSystem fileSystem = null;
    FSDataOutputStream fs = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);
      Path fsPath = new Path(root);

      if (!fileSystem.exists(fsPath)) {
        fileSystem.mkdirs(fsPath);
      }

      Path filePath = new Path(root + "/" + currentPath);
      fs = fileSystem.create(filePath, true);
      fs.write(data);
      fs.hsync();
      // 十进制511 == 八进制777
      fileSystem.setPermission(filePath, new FsPermission((short) 511));
    } finally {
      try {
        if (null != fs) {
          fs.close();
        }
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  public static byte[] readFile(String path, String basePath) throws Exception {
    FileSystem fileSystem = null;
    FSDataInputStream fs = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);
      Path fsPath = new Path(path);

      if (!fileSystem.exists(fsPath)) {
        throw new Exception("Path:" + path + " not exist");
      }
      fs = fileSystem.open(fsPath);
      int fileLen = (int) fileSystem.getFileStatus(fsPath).getLen();
      byte[] data = new byte[fileLen];
      int dataLength = fs.read(data);
      byte[] schemaData = new byte[dataLength];
      System.arraycopy(data, 0, schemaData, 0, dataLength);
      return schemaData;

    } finally {
      try {
        if (null != fs) {
          fs.close();
        }
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }
  //读取文件全部内容
  public static byte[] readFullFile(String path, String basePath) throws Exception {
    FileSystem fileSystem = null;
    FSDataInputStream fs = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);
      Path fsPath = new Path(path);

      if (!fileSystem.exists(fsPath)) {
        throw new Exception("Path:" + path + " not exist");
      }
      fs = fileSystem.open(fsPath);
      int fileLen = (int) fileSystem.getFileStatus(fsPath).getLen();
      byte[] data = new byte[fileLen];
      fs.readFully(0,data);
      return data;
    } finally {
      try {
        if (null != fs) {
          fs.close();
        }
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }
  public static void deleteFile(String path, String basePath) throws Exception {
    FileSystem fileSystem = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);
      Path fsPath = new Path(path);
      if (fileSystem.exists(fsPath)) {
        fileSystem.delete(fsPath,true);
      }

    } finally {
      try {
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  public static void downloadFile(String hdfsFileWithPath, String basePath, String localPath) throws Exception {

    FileSystem localFS = null;
    FileSystem hadoopFS = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      localFS = FileSystem.getLocal(conf);
      hadoopFS = FileSystem.get(conf);
      Path hdfsPath = new Path(hdfsFileWithPath);
      Path local = new Path(localPath);

      hadoopFS.copyToLocalFile(false, hdfsPath, local, true);
    } finally {
      try {
        if (null != hadoopFS) {
          hadoopFS.close();
        }
        if (null != localFS) {
          localFS.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  public static void uploadFile(String hdfsPathName, String basePath, String localFileWithPath) throws Exception {

    FileSystem localFS = null;
    FileSystem hadoopFS = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      localFS = FileSystem.getLocal(conf);
      hadoopFS = FileSystem.get(conf);
      Path hdfsPath = new Path(hdfsPathName);
      Path localPath = new Path(localFileWithPath);

      if(!hadoopFS.exists(hdfsPath)){
        hadoopFS.mkdirs(hdfsPath);
      }

      hadoopFS.copyFromLocalFile(false, true, localPath, hdfsPath);
    } finally {
      try {
        if (null != hadoopFS) {
          hadoopFS.close();
        }
        if (null != localFS) {
          localFS.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  public static Set<String> traversTable(String path, String basePath) throws Exception {
    FileSystem fileSystem = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);
      Path fsPath = new Path(path);

      FileStatus[] fileStatuses = fileSystem.listStatus(fsPath);

      Set<String> tableNames = new HashSet<String>();

      for (FileStatus fs : fileStatuses) {
        tableNames.add(fs.getPath().getName());
      }

      return tableNames;
    } finally {
      try {
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void isExistOrCreate(String path, String basePath) throws Exception {
    FileSystem fileSystem = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);
      Path fsPath = new Path(path);

      if (!fileSystem.exists(fsPath)) {
        fileSystem.mkdirs(fsPath);
      }

      fileSystem.setPermission(fsPath, new FsPermission((short) 511));
    } finally {
      try {
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static boolean isFileExists(String root, String currentPath, String basePath) throws Exception {
    FileSystem fileSystem = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);

      Path filePath = new Path(root + "/" + currentPath);

      return fileSystem.exists(filePath);
    } finally {
      try {
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  /**
   * @param path     文件夹路径
   * @param basePath hdfs原始路径
   * @return 返回二级文件下下的所有文件，形式为<一级文件夹名字， 二级文件名1，二级文件名2，···， 二级文件名n>
   */
  public static Map<String, String> listFiles(String path, String basePath) throws Exception {
    Set<String> folderNames = traversTable(path, basePath);
    Map<String, String> folder2Files = new HashMap<String, String>();
    for (String folderName : folderNames) {
      StringBuilder sFile = new StringBuilder();
      Set<String> files = traversTable(path + "/" + folderName, basePath);
      for (String file : files) {
        sFile.append( file + ",");
        //sFile += file + ",";
      }
      //如果文件夹中没有文件，则不加入map
      if (sFile.length() != 0) {
        folder2Files.put(folderName, sFile.substring(0, sFile.length() - 1));
      }
    }

    return folder2Files;

  }

  public static boolean appendToFile(String filePath,String basePath,String value) throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.support.append", true);
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
    FileSystem fileSystem = null;
    FSDataOutputStream outPutStream = null;
    try {
      FileSystem.setDefaultUri(conf, new URI(basePath));
      fileSystem = FileSystem.get(conf);
      outPutStream = fileSystem.append(new Path(filePath));
      outPutStream.write(Bytes.toBytes(value));
      outPutStream.flush();

    }finally {
      try {
        if (null != outPutStream) {
          outPutStream.close();
        }
        if (null != fileSystem) {
          fileSystem.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    //deleteFile("/tmp/abc","hdfs://hdh230:8020");
  }
}
