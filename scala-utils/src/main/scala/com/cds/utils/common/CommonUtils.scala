package com.cds.utils.common

/**
  * Created by chendongsheng5 on 2017/3/13.
  */
object CommonUtils {
  /**
    * Get the ClassLoader which loaded Spark.
    */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

  /**
    * build stack string
    *
    * @param stackTrace stackTrace element
    * @return stackTrace string
    */
  def stackTraceString(stackTrace: Array[StackTraceElement]): String = {
    val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
    st
  }
}
