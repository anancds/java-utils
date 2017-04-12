package com.cds.utils.component;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

/**
 * Created by chendongsheng5 on 2017/4/12.
 */
public abstract class AbstractComponent {
  protected final Logger logger;
  protected final DeprecationLogger deprecationLogger;
  protected final Settings settings;

  public AbstractComponent(Settings settings) {
    this.logger = Loggers.getLogger(getClass(), settings);
    this.deprecationLogger = new DeprecationLogger(logger);
    this.settings = settings;
  }

  public AbstractComponent(Settings settings, Class customClass) {
    this.logger = LogManager.getLogger(customClass);
    this.deprecationLogger = new DeprecationLogger(logger);
    this.settings = settings;
  }

  public final String nodeName() {
    return Node.NODE_NAME_SETTING.get(settings);
  }

  protected void logDeprecatedSetting(String settingName, String alternativeName) {
    if (!Strings.isNullOrEmpty(settings.get(settingName))) {
      deprecationLogger.deprecated("Setting [{}] is deprecated, use [{}] instead", settingName, alternativeName);
    }
  }

  protected void logRemovedSetting(String settingName, String alternativeName) {
    if (!Strings.isNullOrEmpty(settings.get(settingName))) {
      deprecationLogger.deprecated("Setting [{}] has been removed, use [{}] instead", settingName, alternativeName);
    }
  }
}
