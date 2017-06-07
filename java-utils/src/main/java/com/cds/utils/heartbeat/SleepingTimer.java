package com.cds.utils.heartbeat;

import com.cds.utils.clock.Clock;
import com.cds.utils.clock.SystemClock;
import com.cds.utils.time.Sleeper;
import com.cds.utils.time.ThreadSleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class can be used for executing heartbeats periodically.
 */
@NotThreadSafe
public final class SleepingTimer implements HeartbeatTimer {
  private final long mIntervalMs;
  private long mPreviousTickMs;
  private final String mThreadName;
  private final Logger mLogger;
  private final Clock mClock;
  private final Sleeper mSleeper;

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval
   */
  public SleepingTimer(String threadName, long intervalMs) {
    this(threadName, intervalMs, LoggerFactory.getLogger(SleepingTimer.class),
        new SystemClock(), new ThreadSleeper());
  }

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval
   * @param logger the logger to log to
   * @param clock for telling the current time
   * @param sleeper the utility to use for sleeping
   */
  public SleepingTimer(String threadName, long intervalMs, Logger logger, Clock clock,
      Sleeper sleeper) {
    mIntervalMs = intervalMs;
    mThreadName = threadName;
    mLogger = logger;
    mClock = clock;
    mSleeper = sleeper;
  }

  /**
   * Enforces the thread waits for the given interval between consecutive ticks.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public void tick() throws InterruptedException {
    if (mPreviousTickMs != 0) {
      long executionTimeMs = mClock.millis() - mPreviousTickMs;
      if (executionTimeMs > mIntervalMs) {
        mLogger.warn("{} last execution took {} ms. Longer than the interval {}", mThreadName,
            executionTimeMs, mIntervalMs);
      } else {
        mSleeper.sleep(mIntervalMs - executionTimeMs);
      }
    }
    mPreviousTickMs = mClock.millis();
  }
}
