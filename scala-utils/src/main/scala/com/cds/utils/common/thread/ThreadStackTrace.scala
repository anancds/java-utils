package com.cds.utils.common.thread

/**
  * Used for shipping per-thread stacktraces from the executors to driver.
  */
private[common] case class ThreadStackTrace(
                                            threadId: Long,
                                            threadName: String,
                                            threadState: Thread.State,
                                            stackTrace: String,
                                            blockedByThreadId: Option[Long],
                                            blockedByLock: String,
                                            holdingLocks: Seq[String])
