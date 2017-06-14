package com.cds.utils.common.metrics.sink
import java.util.Properties
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SparkConf}

/**
  * Created by chendongsheng5 on 2017/6/14.
  */
class MetricsServlet(
                      val property: Properties,
                      val registry: MetricRegistry,
                      securityMgr: SecurityManager)
  extends Sink {

  val SERVLET_KEY_PATH = "path"
  val SERVLET_KEY_SAMPLE = "sample"

  val SERVLET_DEFAULT_SAMPLE = false

  val servletPath = property.getProperty(SERVLET_KEY_PATH)

  val servletShowSample = Option(property.getProperty(SERVLET_KEY_SAMPLE)).map(_.toBoolean)
    .getOrElse(SERVLET_DEFAULT_SAMPLE)

  val mapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, servletShowSample))

//  def getHandlers(conf: SparkConf): Array[ServletContextHandler] = {
//    Array[ServletContextHandler](
//      createServletHandler(servletPath,
//        new ServletParams(request => getMetricsSnapshot(request), "text/json"), securityMgr, conf)
//    )
//  }

  def getMetricsSnapshot(request: HttpServletRequest): String = {
    mapper.writeValueAsString(registry)
  }

  override def start() { }

  override def stop() { }

  override def report() { }
}
