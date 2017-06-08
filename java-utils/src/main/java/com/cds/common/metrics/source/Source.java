package com.cds.common.metrics.source;

import com.codahale.metrics.MetricRegistry;

/**
 * Source is where the metrics generated. It uses a {@link MetricRegistry} to register the metrics
 * for monitoring.
 */
public interface Source {
  /**
   * Gets the name of the Source.
   *
   * @return the name of the Source
   */
  String getName();

  /**
   * Gets the instance of the {@link MetricRegistry}. A MetricRegistry is used to register the
   * metrics, and is passed to a Sink so that the sink knows which metrics to report.
   *
   * @return the instance of the MetricRegistry
   */
  MetricRegistry getMetricRegistry();
}
