package com.cds.utils.common.metrics.sink

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.cds.utils.common.metrics.MetricsSystem
import com.codahale.metrics.{MetricRegistry, Slf4jReporter}
import org.apache.spark.SecurityManager

/**
  * Created by chendongsheng5 on 2017/6/14.
  */
class Slf4jSink(
                 val property: Properties,
                 val registry: MetricRegistry,
                 securityMgr: SecurityManager)
  extends Sink {
  val SLF4J_DEFAULT_PERIOD = 10
  val SLF4J_DEFAULT_UNIT = "SECONDS"

  val SLF4J_KEY_PERIOD = "period"
  val SLF4J_KEY_UNIT = "unit"

  val pollPeriod = Option(property.getProperty(SLF4J_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => SLF4J_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(SLF4J_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(SLF4J_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: Slf4jReporter = Slf4jReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build()

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
