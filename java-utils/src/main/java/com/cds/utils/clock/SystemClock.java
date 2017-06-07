package com.cds.utils.clock;

/**
 * A clock representing the current time as reported by the operating system.
 */
public final class SystemClock implements Clock {
  /**
   * Constructs a new {@link Clock} which reports the actual time.
   */
  public SystemClock() {}

  @Override
  public long millis() {
    return System.currentTimeMillis();
  }
}
