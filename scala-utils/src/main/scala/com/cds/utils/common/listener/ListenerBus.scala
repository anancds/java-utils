package com.cds.utils.common.listener

import java.util.concurrent.CopyOnWriteArrayList

import com.cds.utils.common.Logging

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal


/**
  * An event bus which posts events to its listeners.
  */
private[common] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // Marked `private[spark]` for access in tests.
  private[common] val listeners = new CopyOnWriteArrayList[L]

  /**
    * Add a listener to listen events. This method is thread-safe and can be called in any thread.
    */
  final def addListener(listener: L): Unit = {
    listeners.add(listener)
  }

  /**
    * Remove a listener and it won't receive any events. This method is thread-safe and can be called
    * in any thread.
    */
  final def removeListener(listener: L): Unit = {
    listeners.remove(listener)
  }

  /**
    * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
    * `postToAll` in the same thread for all events.
    */
  def postToAll(event: E): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        doPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  /**
    * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
    * thread for all listeners.
    */
  protected def doPostEvent(listener: L, event: E): Unit

  private[common] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
