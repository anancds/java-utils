package com.cds.utils.common.metrics.sink

import java.util.Properties

import com.codahale.metrics.{JmxReporter, MetricRegistry}
import org.apache.spark.SecurityManager

/**
  * Created by chendongsheng5 on 2017/6/14.
  */
class JmxSink(val property: Properties, val registry: MetricRegistry,
              securityMgr: SecurityManager) extends Sink {

  val reporter: JmxReporter = JmxReporter.forRegistry(registry).build()

  override def start() {
    reporter.start()
  }

  override def stop() {
    reporter.stop()
  }

  override def report() { }

}
