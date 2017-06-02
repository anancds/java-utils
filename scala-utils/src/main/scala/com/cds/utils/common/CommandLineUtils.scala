package com.cds.utils.common

import java.io.PrintStream

/**
  * Created by chendongsheng5 on 2017/6/1.
  */
private[common] trait CommandLineUtils {

  // Exposed for testing
  private[common] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)

  private[common] var printStream: PrintStream = System.err

  // scalastyle:off println

  private[common] def printWarning(str: String): Unit = printStream.println("Warning: " + str)

  private[common] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }

  // scalastyle:on println

  private[common] def parseSparkConfProperty(pair: String): (String, String) = {
    pair.split("=", 2).toSeq match {
      case Seq(k, v) => (k, v)
      case _ => printErrorAndExit(s"Spark config without '=': $pair")
        throw new Exception(s"Spark config without '=': $pair")
    }
  }

  def main(args: Array[String]): Unit
}
