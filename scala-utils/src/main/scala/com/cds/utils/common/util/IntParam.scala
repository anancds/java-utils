package com.cds.utils.common.util

/**
  * An extractor object for parsing strings into integers.
  */
private[common] object IntParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def main(args: Array[String]): Unit = {
    println("a")
    val Array(host, IntParam(port)) = Array("127.0.0.1, 123", "12")
    println(port)
  }
}
