package com.cds.utils.resource;

import java.util.concurrent.locks.Lock;

/**
 * A resource lock that makes it possible to acquire and release locks using the following idiom:
 *
 * <pre>
 *   try (LockResource r = new LockResource(lock)) {
 *     ...
 *   }
 * </pre>
 */
public class LockResource implements AutoCloseable {
  private final Lock mLock;

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   */
  public LockResource(Lock lock) {
    mLock = lock;
    mLock.lock();
  }

  /**
   * Releases the lock.
   */
  @Override
  public void close() {
    mLock.unlock();
  }
}
