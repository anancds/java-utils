package com.cds.utils.time;

public interface Sleeper {

  /**
   * Sleeps for the given number of milliseconds.
   *
   * @param millis the number of milliseconds to sleep for
   * @throws InterruptedException if the sleep is interrupted
   */
  void sleep(long millis) throws InterruptedException;
}
