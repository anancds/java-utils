package com.cds.utils.heartbeat;

/**
 * An interface for heartbeat timers. The {@link HeartbeatThread} calls the {@link #tick()} method.
 */
public interface HeartbeatTimer {
  /**
   * Waits until next heartbeat should be executed.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  void tick() throws InterruptedException;
}
