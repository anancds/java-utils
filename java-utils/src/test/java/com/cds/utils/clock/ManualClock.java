package com.cds.utils.clock;

/**
 * A manually set clock useful for testing.
 */
public final class ManualClock implements Clock {

  private long mTimeMs;

  /**
   * Constructs a {@link ManualClock} set to the current system time.
   */
  public ManualClock() {
    this(System.currentTimeMillis());
  }

  /**
   * Constructs a {@link ManualClock} set to the specified time.
   *
   * @param time the time to set the clock to
   */
  public ManualClock(long time) {
    mTimeMs = time;
  }

  /**
   * Sets the clock to the specified time.
   *
   * @param timeMs the time to set the clock to
   */
  public synchronized void setTimeMs(long timeMs) {
    mTimeMs = timeMs;
  }

  /**
   * Moves the clock forward the specified amount of time.
   *
   * @param timeMs the time to add in milliseconds
   */
  public synchronized void addTimeMs(long timeMs) {
    mTimeMs += timeMs;
  }

  @Override
  public synchronized long millis() {
    return mTimeMs;
  }
}