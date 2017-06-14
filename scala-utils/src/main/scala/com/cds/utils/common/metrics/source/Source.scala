package com.cds.utils.common.metrics.source

import com.codahale.metrics.MetricRegistry

/**
  * Created by chendongsheng5 on 2017/6/14.
  */
trait Source {
  def sourceName: String
  def metricRegistry: MetricRegistry
}
