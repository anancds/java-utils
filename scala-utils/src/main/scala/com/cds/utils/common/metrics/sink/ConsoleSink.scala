package com.cds.utils.common.metrics.sink

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.cds.utils.common.metrics.MetricsSystem
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}

/**
  * Created by chendongsheng5 on 2017/6/14.
  */
class ConsoleSink(val property: Properties, val registry: MetricRegistry,
                  securityMgr: SecurityManager) extends Sink {
  val CONSOLE_DEFAULT_PERIOD = 10
  val CONSOLE_DEFAULT_UNIT = "SECONDS"

  val CONSOLE_KEY_PERIOD = "period"
  val CONSOLE_KEY_UNIT = "unit"

  val pollPeriod = Option(property.getProperty(CONSOLE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CONSOLE_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(CONSOLE_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(CONSOLE_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: ConsoleReporter = ConsoleReporter.forRegistry(registry)
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
