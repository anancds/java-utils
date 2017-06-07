package com.cds.utils.time;

/**
 * A sleeping utility which delegates to Thread.sleep().
 */
public class ThreadSleeper implements Sleeper {

  /**
   * Constructs a new {@link ThreadSleeper}.
   */
  public ThreadSleeper() {}

  @Override
  public void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }
}
