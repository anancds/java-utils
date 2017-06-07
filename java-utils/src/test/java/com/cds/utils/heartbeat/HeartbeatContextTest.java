package com.cds.utils.heartbeat;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link HeartbeatContext}.
 */
public final class HeartbeatContextTest {

  @Test
  public void allThreadsUseSleepingTimer() {
    for (String threadName : HeartbeatContext.getTimerClasses().keySet()) {
      Class<? extends HeartbeatTimer> timerClass = HeartbeatContext.getTimerClass(threadName);
      Assert.assertTrue(timerClass.isAssignableFrom(SleepingTimer.class));
    }
  }

  @Test
  public void canTemporarilySwitchToScheduledTimer() throws Exception {
    try (ManuallyScheduleHeartbeat.Resource h =
        new ManuallyScheduleHeartbeat.Resource(ImmutableList.of(HeartbeatContext.WORKER_CLIENT))) {
      Assert.assertTrue(HeartbeatContext.getTimerClass(HeartbeatContext.WORKER_CLIENT)
          .isAssignableFrom(ScheduledTimer.class));
    }
    Assert.assertTrue(HeartbeatContext.getTimerClass(HeartbeatContext.WORKER_CLIENT)
        .isAssignableFrom(SleepingTimer.class));
  }
}
