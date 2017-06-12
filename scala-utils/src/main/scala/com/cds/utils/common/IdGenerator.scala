package com.cds.utils.common

import java.util.concurrent.atomic.AtomicInteger

/**
  * Created by chendongsheng5 on 2017/6/9.
  */
private[common] class IdGenerator {
  private val id = new AtomicInteger
  def next: Int = id.incrementAndGet
}
