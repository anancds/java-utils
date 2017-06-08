package com.cds.common.metrics;

import com.codahale.metrics.Counter;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MetricsSystem}.
 */
public final class MetricsSystemTest {

  private static Counter sCounter =
      MetricsSystem.METRIC_REGISTRY.counter(MetricsSystem.getMasterMetricName("counter"));
  private MetricsConfig mMetricsConfig;

  /**
   * Sets up the properties for the configuration of the metrics before a test runs.
   */
  @Before
  public final void before() {
    Properties metricsProps = new Properties();
    metricsProps.setProperty("sink.console.class", "com.cds.common.metrics.sink.ConsoleSink");
    metricsProps.setProperty("sink.console.period", "20");
    metricsProps.setProperty("sink.console.period", "20");
    metricsProps.setProperty("sink.console.unit", "minutes");
    metricsProps.setProperty("sink.jmx.class", "com.cds.common.metrics.sink.JmxSink");
    mMetricsConfig = new MetricsConfig(metricsProps);
  }

  /**
   * Tests the metrics for a master and a worker.
   */
  @Test
  public void metricsSystem() {
    MetricsSystem.startSinksFromConfig(mMetricsConfig);

    Assert.assertEquals(2, MetricsSystem.getNumSinks());

    // Make sure it doesn't crash.
    sCounter.inc();
    MetricsSystem.stopSinks();
  }
}