package com.cds.utils.common

import scala.util.Properties

/**
  * Created by chendongsheng5 on 2017/6/1.
  */
object Test extends CommandLineUtils{

  def main(args: Array[String]): Unit = {
    printStream.println("abc")

    printStream.println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format("2.2"))

    printStream.println("Using Scala %s, %s, %s".format(
      Properties.versionString, Properties.javaVmName, Properties.javaVersion))
  }


}
