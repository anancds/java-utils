package com.cds.common.metrics.source;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A Source which collects JVM metrics, including JVM memory usage, GC counts, GC times, etc.
 */
@ThreadSafe
public class JvmSource implements Source {
  private static final String JVM_SOURCE_NAME = "jvm";
  private final MetricRegistry mMetricRegistry;

  /**
   * Creates a new {@link JvmSource} and register all JVM metrics.
   */
  public JvmSource() {
    mMetricRegistry = new MetricRegistry();
    mMetricRegistry.registerAll(new GarbageCollectorMetricSet());
    mMetricRegistry.registerAll(new MemoryUsageGaugeSet());
  }

  @Override
  public String getName() {
    return JVM_SOURCE_NAME;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }
}
