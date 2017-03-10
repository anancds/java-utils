package com.cds.utils.zookeeper;

import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chendongsheng5 on 2017/3/10.
 */
@SuppressWarnings("unused")
public class ZookeeperUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperUtil.class);
  private CuratorFramework client;

  public ZookeeperUtil(CuratorFramework client) {
    this.client = client;
  }

  public ZookeeperUtil(String connectionString, String namespace) {
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    this.client = CuratorFrameworkFactory.builder().connectString(connectionString)
        .namespace(namespace).
            retryPolicy(retryPolicy).connectionTimeoutMs(2000).sessionTimeoutMs(10000).build();
    this.client.start();
  }

  public ZookeeperUtil(String connectionString, String namespace, int baseSleepTimeMs,
      int maxRetries, int connectionTimeoutMs, int sessionTimeoutMs) {
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
    this.client = CuratorFrameworkFactory.builder().connectString(connectionString)
        .namespace(namespace).
            retryPolicy(retryPolicy).connectionTimeoutMs(connectionTimeoutMs)
        .sessionTimeoutMs(sessionTimeoutMs).build();
    this.client.start();
  }

  /**
   * 获取curator client
   */
  public CuratorFramework getClient() {
    return client;
  }

  /**
   * 判断zookeeper中目录节点是否已存在
   */
  public boolean isNodeExist(String path) {
    try {
      if (client.checkExists().forPath(path) != null) {
        return true;
      }
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return false;
  }

  /**
   * 在zookeeper中添加节点
   * 注：目录中该节点必须不存在，而且父节点必须存在，否则抛异常
   */
  public boolean createNode(String path) {
    try {
      client.create().withMode(CreateMode.PERSISTENT).forPath(path);
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return false;
  }

  /**
   * 在zookeeper中添加节点
   * 注：目录中该节点必须不存在，而且父节点必须存在，否则抛异常
   */
  public boolean createNode(String path, byte[] data) {
    try {
      client.create().withMode(CreateMode.PERSISTENT).forPath(path, data);
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return false;
  }

  /**
   * 在zookeeper中添加节点
   * 注：目录中该节点必须不存在，而且父节点必须存在，否则抛异常
   */
  public boolean createTempNode(String path, byte[] data) {
    try {
      client.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return false;
  }

  /**
   * 在zookeeper删除节点
   * 注：目录中该节点必须存在，否则抛异常
   */
  public boolean deleteNode(String path) {
    try {
      client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return false;
  }

  /**
   * 在zookeeper中更新节点数据
   */
  public boolean setData(String path, byte[] payload) {
    try {
      client.setData().forPath(path, payload);
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return false;
  }

  /**
   * 从zookeeper中获取节点数据
   */
  public byte[] getData(String path) {
    try {
      return client.getData().forPath(path);
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return null;
  }

  /**
   * 从zookeeper中获取子节点集合
   */
  public List<String> getChildren(String path) {
    try {
      return client.getChildren().forPath(path);
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return null;
  }

  public PathChildrenCache setPathCache(String path, boolean isCache,
      PathChildrenCacheListener listener) {
    PathChildrenCache cache = new PathChildrenCache(client, path, isCache);
    cache.getListenable().addListener(listener);
    try {
      cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      LOG.warn("Cache start failed for:" + path);
      return null;
    }
    LOG.debug("Set listener for path: " + path);
    return cache;
  }

  public void close() {
    client.close();
  }
}
