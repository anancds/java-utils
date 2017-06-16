package com.cds.common.retry;


import org.junit.Assert;
import org.junit.Test;

/**
 * Created by chendongsheng5 on 2017/6/16.
 */
public class CountingRetryTest {

  @Test
  public void testNumRetries() {
    int numTries = 10;
    CountingRetry countingRetry = new CountingRetry(numTries);
    Assert.assertEquals(0, countingRetry.getRetryCount());
    int retryAttempts = 0;
    while (countingRetry.attemptRetry()) {
      retryAttempts++;
    }
    Assert.assertEquals(numTries, retryAttempts);
    Assert.assertEquals(numTries, countingRetry.getRetryCount());
  }
}
