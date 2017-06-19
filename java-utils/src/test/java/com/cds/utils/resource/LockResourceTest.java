package com.cds.utils.resource;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by chendongsheng5 on 2017/6/16.
 */
public final class LockResourceTest {

  /**
   * Tests {@link LockResource} with {@link ReentrantLock}.
   */
  @Test
  public void reentrantLock() {
    Lock lock = new ReentrantLock();
    try (LockResource r1 = new LockResource(lock)) {
      try (LockResource r2 = new LockResource(lock)) {
        Assert.assertTrue(lock.tryLock());
        lock.unlock();
      }
    }
  }

  /**
   * Tests {@link LockResource} with {@link ReentrantReadWriteLock}.
   */
  @Test
  public void reentrantReadWriteLock() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    try (LockResource r1 = new LockResource(lock.readLock())) {
      try (LockResource r2 = new LockResource(lock.readLock())) {
        Assert.assertEquals(lock.getReadHoldCount(), 2);
        Assert.assertTrue(lock.readLock().tryLock());
        lock.readLock().unlock();
      }
    }
    Assert.assertEquals(lock.getReadHoldCount(), 0);

    try (LockResource r1 = new LockResource(lock.writeLock())) {
      try (LockResource r2 = new LockResource(lock.readLock())) {
        Assert.assertTrue(lock.isWriteLockedByCurrentThread());
        Assert.assertEquals(lock.getReadHoldCount(), 1);
      }
    }
    Assert.assertFalse(lock.isWriteLockedByCurrentThread());
    Assert.assertEquals(lock.getReadHoldCount(), 0);

    try (LockResource r = new LockResource(lock.readLock())) {
      Assert.assertFalse(lock.writeLock().tryLock());
    }
  }
}