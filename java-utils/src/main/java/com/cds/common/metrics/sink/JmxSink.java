package com.cds.common.metrics.sink;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A sink which listens for new metrics and exposes them as namespaces MBeans.
 */
@ThreadSafe
public final class JmxSink implements Sink {
  private JmxReporter mReporter;

  /**
   * Creates a new {@link JmxSink} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param properties the properties
   * @param registry the metric registry to register
   */
  public JmxSink(Properties properties, MetricRegistry registry) {
    mReporter = JmxReporter.forRegistry(registry).build();
  }

  @Override
  public void start() {
    mReporter.start();
  }

  @Override
  public void stop() {
    mReporter.stop();
  }

  @Override
  public void report() {
  }
}
