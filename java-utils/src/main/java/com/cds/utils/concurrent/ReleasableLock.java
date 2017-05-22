package com.cds.utils.concurrent;

import com.cds.utils.lease.Releasable;
import java.util.concurrent.locks.Lock;
import org.elasticsearch.index.engine.EngineException;

/**
 * Created by chendongsheng5 on 2017/5/22.
 */
public class ReleasableLock implements Releasable {
  private final Lock lock;

  /* a per thread boolean indicating the lock is held by it. only works when assertions are enabled */
  private final ThreadLocal<Boolean> holdingThreads;

  public ReleasableLock(Lock lock) {
    this.lock = lock;
    boolean useHoldingThreads = false;
    assert (useHoldingThreads = true);
    if (useHoldingThreads) {
      holdingThreads = new ThreadLocal<>();
    } else {
      holdingThreads = null;
    }
  }

  @Override
  public void close() {
    lock.unlock();
    assert removeCurrentThread();
  }


  public ReleasableLock acquire() throws EngineException {
    lock.lock();
    assert addCurrentThread();
    return this;
  }

  private boolean addCurrentThread() {
    holdingThreads.set(true);
    return true;
  }

  private boolean removeCurrentThread() {
    holdingThreads.remove();
    return true;
  }

  public Boolean isHeldByCurrentThread() {
    if (holdingThreads == null) {
      throw new UnsupportedOperationException("asserts must be enabled");
    }
    Boolean b = holdingThreads.get();
    return b != null && b.booleanValue();
  }
}
