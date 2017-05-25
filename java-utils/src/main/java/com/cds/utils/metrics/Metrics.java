package com.cds.utils.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;

/**
 * Created by chendongsheng5 on 2017/5/25.
 */
public interface Metrics extends MetricSet {

  Counter counter(String name);

  Timer timer(String name);

  Meter meter(String name);

}
