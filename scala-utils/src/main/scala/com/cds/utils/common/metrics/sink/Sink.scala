package com.cds.utils.common.metrics.sink

/**
  * Created by chendongsheng5 on 2017/6/14.
  */
private[common] trait Sink {
  def start(): Unit
  def stop(): Unit
  def report(): Unit
}
