package com.cds.utils.heartbeat;

import java.io.Closeable;

/**
 * Created by chendongsheng5 on 2017/6/7.
 */
public interface HeartbeatExecutor extends Closeable {

  /**
   * Implements the heartbeat logic.
   *
   * @throws InterruptedException if the thread is interrupted
   */
  void heartbeat() throws InterruptedException;

  /**
   * Cleans up any resources used by the heartbeat executor.
   */
  @Override
  void close();
}
