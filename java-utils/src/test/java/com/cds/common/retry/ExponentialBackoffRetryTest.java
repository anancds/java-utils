package com.cds.common.retry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link ExponentialBackoffRetry} class.
 */
public final class ExponentialBackoffRetryTest {

  /**
   * Ensures that a lot of retries always produce a positive time.
   */
  @Test
  public void largeRetriesProducePositiveTime() {
    int max = 1000;
    MockExponentialBackoffRetry backoff =
        new MockExponentialBackoffRetry(50, Integer.MAX_VALUE, max);
    for (int i = 0; i < max; i++) {
      backoff.setRetryCount(i);
      long time = backoff.getSleepTime();
      Assert.assertTrue("Time must always be positive: " + time, time > 0);
    }
  }

  /**
   * Mocks the {@link ExponentialBackoffRetry} class.
   */
  public static final class MockExponentialBackoffRetry extends ExponentialBackoffRetry {
    private int mRetryCount = 0;

    /**
     * Constructs a new mock.
     *
     * @param baseSleepTimeMs the basic sleep time in milliseconds
     * @param maxSleepMs the max sleep time in milliseconds
     * @param maxRetries the max count of retries
     */
    public MockExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepMs, int maxRetries) {
      super(baseSleepTimeMs, maxSleepMs, maxRetries);
    }

    @Override
    public int getRetryCount() {
      return mRetryCount;
    }

    /**
     * Sets the count of retries.
     *
     * @param retryCount the count of retries
     */
    public void setRetryCount(int retryCount) {
      mRetryCount = retryCount;
    }
  }
}