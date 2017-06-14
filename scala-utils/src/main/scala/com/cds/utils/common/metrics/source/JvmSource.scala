package com.cds.utils.common.metrics.source

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm.{GarbageCollectorMetricSet, MemoryUsageGaugeSet}

/**
  * Created by chendongsheng5 on 2017/6/14.
  */
class JvmSource extends Source {
  override val sourceName = "jvm"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.registerAll(new GarbageCollectorMetricSet)
  metricRegistry.registerAll(new MemoryUsageGaugeSet)
}
