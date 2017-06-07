package com.cds.utils.heartbeat;

import com.google.common.base.Throwables;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A rule which will temporarily change a heartbeat to being manually scheduled. See
 * {@link HeartbeatScheduler}.
 */
public final class ManuallyScheduleHeartbeat implements TestRule {

  private final List<String> mThreads;

  /**
   * @param threads names of the threads to manually schedule; names are defined in {@link
   * HeartbeatContext}
   */
  public ManuallyScheduleHeartbeat(List<String> threads) {
    mThreads = threads;
  }

  /**
   * @param threads names of the threads to manually schedule; names are defined in {@link
   * HeartbeatContext}
   */
  public ManuallyScheduleHeartbeat(String... threads) {
    this(Arrays.asList(threads));
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (Resource resource = new Resource(mThreads)) {
          statement.evaluate();
        }
      }
    };
  }

  /**
   * Stores executor threads and corresponding timer class.
   */
  public static class Resource implements AutoCloseable {

    private final List<String> mThreads;
    private final Map<String, Class<? extends HeartbeatTimer>> mPrevious;

    public Resource(List<String> threads) {
      mThreads = threads;
      mPrevious = new HashMap<>();
      for (String threadName : mThreads) {
        try {
          mPrevious.put(threadName, HeartbeatContext.getTimerClass(threadName));
          Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", threadName,
              HeartbeatContext.SCHEDULED_TIMER_CLASS);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }

    @Override
    public void close() throws Exception {
      for (String threadName : mThreads) {
        Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", threadName,
            mPrevious.get(threadName));
      }
    }
  }
}
