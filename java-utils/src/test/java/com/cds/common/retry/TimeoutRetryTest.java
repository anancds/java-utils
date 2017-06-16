package com.cds.common.retry;

import org.junit.Assert;
import org.junit.Test;
/**
 * Created by chendongsheng5 on 2017/6/16.
 */
public class TimeoutRetryTest {
  @Test
  public void timeout() {
    final long timeoutMs = 5000;
    final int sleepMs = 100;
    int attempts = 0;
    TimeoutRetry timeoutRetry = new TimeoutRetry(timeoutMs, sleepMs);
    Assert.assertEquals(0, timeoutRetry.getRetryCount());
    long startMs = System.currentTimeMillis();
    while (timeoutRetry.attemptRetry()) {
      attempts++;
    }
    long endMs = System.currentTimeMillis();
    Assert.assertTrue(attempts > 0);
    Assert.assertTrue((endMs - startMs) >= timeoutMs);
    Assert.assertEquals(attempts, timeoutRetry.getRetryCount());
    Assert.assertTrue(attempts <= (timeoutMs / sleepMs) + 1);
  }
}