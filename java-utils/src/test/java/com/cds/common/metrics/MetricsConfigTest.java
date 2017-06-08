package com.cds.common.metrics;

import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MetricsConfig}.
 */
public final class MetricsConfigTest {

  private Properties mMetricsProps;

  /**
   * Sets up the properties for the configuration of the metrics before a test runs.
   */
  @Before
  public final void before() {
    mMetricsProps = new Properties();
    mMetricsProps.setProperty("sink.console.class", "com.cds.common.metrics.sink.ConsoleSink");
    mMetricsProps.setProperty("sink.console.period", "15");
    mMetricsProps.setProperty("*.sink.console.unit", "minutes");
    mMetricsProps.setProperty("sink.jmx.class", "com.cds.common.metrics.sink.JmxSink");
  }

  /**
   * Tests that the {@link MetricsConfig#MetricsConfig(Properties)} constructor sets the properties
   * correctly.
   */
  @Test
  public void setProperties() {
    MetricsConfig config = new MetricsConfig(mMetricsProps);

    Properties masterProp = config.getProperties();
    Assert.assertEquals(4, masterProp.size());
    Assert.assertEquals("com.cds.common.metrics.sink.ConsoleSink",
        masterProp.getProperty("sink.console.class"));
    Assert.assertEquals("15", masterProp.getProperty("sink.console.period"));
    Assert.assertEquals("minutes", masterProp.getProperty("sink.console.unit"));
    Assert.assertEquals("com.cds.common.metrics.sink.JmxSink",
        masterProp.getProperty("sink.jmx.class"));
  }

  /**
   * Tests the {@link MetricsConfig#subProperties(Properties, String)} method.
   */
  @Test
  public void subProperties() {
    MetricsConfig config = new MetricsConfig(mMetricsProps);

    Properties properties = config.getProperties();

    Map<String, Properties> sinkProps =
        MetricsConfig.subProperties(properties, MetricsSystem.SINK_REGEX);
    Assert.assertEquals(2, sinkProps.size());
    Assert.assertTrue(sinkProps.containsKey("console"));
    Assert.assertTrue(sinkProps.containsKey("jmx"));

    Properties consoleProp = sinkProps.get("console");
    Assert.assertEquals(3, consoleProp.size());
    Assert
        .assertEquals("com.cds.common.metrics.sink.ConsoleSink", consoleProp.getProperty("class"));

    Properties jmxProp = sinkProps.get("jmx");
    Assert.assertEquals(1, jmxProp.size());
    Assert.assertEquals("com.cds.common.metrics.sink.JmxSink", jmxProp.getProperty("class"));
  }
}
