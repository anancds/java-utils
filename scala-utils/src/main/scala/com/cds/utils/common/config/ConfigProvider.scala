package com.cds.utils.common.config

import java.util.{Map => JMap}

/**
  * Created by chendongsheng5 on 2017/6/5.
  */
private[spark] trait ConfigProvider {

  def get(key: String): Option[String]

}

private[spark] class EnvProvider extends ConfigProvider {

  override def get(key: String): Option[String] = sys.env.get(key)

}

private[spark] class SystemProvider extends ConfigProvider {

  override def get(key: String): Option[String] = sys.props.get(key)

}

private[spark] class MapProvider(conf: JMap[String, String]) extends ConfigProvider {

  override def get(key: String): Option[String] = Option(conf.get(key))

}

/**
  * A config provider that only reads Spark config keys, and considers default values for known
  * configs when fetching configuration values.
  */
private[spark] class SparkConfigProvider(conf: JMap[String, String]) extends ConfigProvider {

  import ConfigEntry._

  override def get(key: String): Option[String] = {
    if (key.startsWith("spark.")) {
      Option(conf.get(key)).orElse(defaultValueString(key))
    } else {
      None
    }
  }

  private def defaultValueString(key: String): Option[String] = {
    findEntry(key) match {
      case e: ConfigEntryWithDefault[_] => Option(e.defaultValueString)
      case e: ConfigEntryWithDefaultString[_] => Option(e.defaultValueString)
      case e: FallbackConfigEntry[_] => get(e.fallback.key)
      case _ => None
    }
  }

}
