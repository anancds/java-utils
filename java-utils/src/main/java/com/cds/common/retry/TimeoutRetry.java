package com.cds.common.retry;

import com.cds.common.CommonUtils;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A retry policy which allows retrying until a specified timeout is reached.
 */
@NotThreadSafe
public class TimeoutRetry implements RetryPolicy {

  private final long mRetryTimeoutMs;
  private final long mSleepMs;
  private long mStartMs = 0;
  private int mCount = 0;

  /**
   * Constructs a retry facility which allows retrying until a specified timeout is reached.
   *
   * @param retryTimeoutMs maximum period of time to retry for, in milliseconds
   * @param sleepMs time in milliseconds to sleep before retrying
   */
  public TimeoutRetry(long retryTimeoutMs, int sleepMs) {
    Preconditions.checkArgument(retryTimeoutMs > 0, "Retry timeout must be a positive number");
    Preconditions.checkArgument(sleepMs >= 0, "sleepMs cannot be negative");
    mRetryTimeoutMs = retryTimeoutMs;
    mSleepMs = sleepMs;
  }

  @Override
  public int getRetryCount() {
    return mCount;
  }

  @Override
  public boolean attemptRetry() {
    if (mCount == 0) {
      // first attempt
      mStartMs = CommonUtils.getCurrentMs();
      mCount++;
      return true;
    }

    if (mSleepMs > 0) {
      CommonUtils.sleepMs(mSleepMs);
    }
    if ((CommonUtils.getCurrentMs() - mStartMs) <= mRetryTimeoutMs) {
      mCount++;
      return true;
    }
    return false;
  }
}
