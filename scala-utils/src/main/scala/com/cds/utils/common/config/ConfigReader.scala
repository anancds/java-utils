package com.cds.utils.common.config

import java.util.{Map => JMap}


import scala.collection.mutable.HashMap
import scala.util.matching.Regex

/**
  * Created by chendongsheng5 on 2017/6/6.
  *
  *
  */

private object ConfigReader {

  private val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r

}

private[common] class ConfigReader(conf: ConfigProvider) {

  def this(conf: JMap[String, String]) = this(new MapProvider(conf))

  private val bindings = new HashMap[String, ConfigProvider]()
  bind(null, conf)
  bindEnv(new EnvProvider())
  bindSystem(new SystemProvider())

  /**
    * Binds a prefix to a provider. This method is not thread-safe and should be called
    * before the instance is used to expand values.
    */
  def bind(prefix: String, provider: ConfigProvider): ConfigReader = {
    bindings(prefix) = provider
    this
  }

  def bind(prefix: String, values: JMap[String, String]): ConfigReader = {
    bind(prefix, new MapProvider(values))
  }

  def bindEnv(provider: ConfigProvider): ConfigReader = bind("env", provider)

  def bindSystem(provider: ConfigProvider): ConfigReader = bind("system", provider)

  /**
    * Reads a configuration key from the default provider, and apply variable substitution.
    */
  def get(key: String): Option[String] = conf.get(key).map(substitute)

  /**
    * Perform variable substitution on the given input string.
    */
  def substitute(input: String): String = substitute(input, Set())

  private def substitute(input: String, usedRefs: Set[String]): String = {
    if (input != null) {
      ConfigReader.REF_RE.replaceAllIn(input, { m =>
        val prefix = m.group(1)
        val name = m.group(2)
        val ref = if (prefix == null) name else s"$prefix:$name"
        require(!usedRefs.contains(ref), s"Circular reference in $input: $ref")

        val replacement = bindings.get(prefix)
          .flatMap(_.get(name))
          .map { v => substitute(v, usedRefs + ref) }
          .getOrElse(m.matched)
        Regex.quoteReplacement(replacement)
      })
    } else {
      input
    }
  }

}
