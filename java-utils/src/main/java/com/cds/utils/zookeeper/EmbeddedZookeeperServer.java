package com.cds.utils.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chendongsheng5 on 2017/3/10.
 */
@SuppressWarnings("unused")
public class EmbeddedZookeeperServer {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedZookeeperServer.class);

  private final AtomicBoolean shutdown;
  private final int clientPort;
  private final File dataDir;
  private final boolean isTemporary;
  private final long tickTime;

  private final Thread serverThread;
  private final NIOServerCnxnFactory connectionFactory;

  EmbeddedZookeeperServer(Integer clientPort, File dataDir, Long tickTime) throws
      QuorumPeerConfig.ConfigException, IOException {
    Preconditions.checkNotNull(clientPort);
    Preconditions.checkNotNull(tickTime);

    Preconditions.checkArgument(clientPort > 0 && clientPort < 65536);
    Preconditions.checkArgument(tickTime > 0);

    this.shutdown = new AtomicBoolean(false);
    this.clientPort = clientPort;
    this.isTemporary = dataDir == null;
    if (this.isTemporary) {
      this.dataDir = Files.createTempDir();
    } else {
      this.dataDir = dataDir;
    }

    this.tickTime = tickTime;

    Properties properties = new Properties();
    properties.setProperty("tickTime", tickTime.toString());
    properties.setProperty("clientPort", clientPort.toString());
    properties.setProperty("dataDir", this.dataDir.getAbsolutePath());

    QuorumPeerConfig qpc = new QuorumPeerConfig();
    try {
      qpc.parseProperties(properties);
    } catch (IOException e) {
      throw new RuntimeException(
          "This is impossible - no I/O to configure a quorumpeer from a properties object", e);
    }

    ServerConfig config = new ServerConfig();
    config.readFrom(qpc);

    log.info("Starting embedded zookeeper server on port " + clientPort);
    ZooKeeperServer zooKeeperServer = new ZooKeeperServer();
    configure(zooKeeperServer, config);

    this.connectionFactory = new NIOServerCnxnFactory();
    this.connectionFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
    try {
      this.connectionFactory.startup(zooKeeperServer);
    } catch (InterruptedException e) {
      throw new RuntimeException("Server Interrupted", e);
    }

    this.serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          EmbeddedZookeeperServer.this.connectionFactory.join();
        } catch (InterruptedException e) {
          log.error("Zookeeper Connection Factory Interrupted", e);
        }
      }
    });

    this.serverThread.start();
  }

  static Builder builder() {
    return new Builder();
  }

  private void configure(ZooKeeperServer zooKeeperServer, ServerConfig config) throws IOException {
    zooKeeperServer.setTxnLogFactory(
        new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir())));
    zooKeeperServer.setTickTime(config.getTickTime());
    zooKeeperServer.setMinSessionTimeout(config.getMinSessionTimeout());
    zooKeeperServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
  }

  /**
   * Shuts down this server instance.
   */
  void shutdown() throws InterruptedException {
    boolean alreadyShutdown = this.shutdown.getAndSet(true);

    if (!alreadyShutdown) {
      this.connectionFactory.shutdown();

      this.serverThread.join();

      if (this.isTemporary) {
        try {
          FileUtils.deleteDirectory(this.dataDir);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * 返回zookeeper服务的监听端口
   *
   * @return 监听端口
   */
  public int getClientPort() {
    return this.clientPort;
  }

  /**
   * 返回zookeeper服务的数据目录
   *
   * @return 返回zookeeper服务用来存储snapshots和一些事务日志的目录
   */
  public File getDataDir() {
    return this.dataDir;
  }

  /**
   * 返回zookeeper服务的tickTime
   *
   * @return tickTime
   */
  public long getTickTime() {
    return this.tickTime;
  }

  public static class Builder {

    private int clientPort = 2181;
    private File dataDir = null;
    private long tickTime = 2000;

    public Builder() {
    }

    Builder clientPort(int port) {
      this.clientPort = port;
      return this;
    }

    public Builder dataDir(File dataDir) {
      this.dataDir = dataDir;
      return this;
    }

    public Builder tickTime(Long tickTime) {
      this.tickTime = tickTime;
      return this;
    }

    EmbeddedZookeeperServer build() throws IOException, QuorumPeerConfig.ConfigException {
      return new EmbeddedZookeeperServer(this.clientPort, this.dataDir, this.tickTime);
    }
  }
}
