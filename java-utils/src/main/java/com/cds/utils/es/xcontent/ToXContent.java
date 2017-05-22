package com.cds.utils.es.xcontent;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent.MapParams;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Created by chendongsheng5 on 2017/5/22.
 */
public interface ToXContent {
  interface Params {
    String param(String key);

    String param(String key, String defaultValue);

    boolean paramAsBoolean(String key, boolean defaultValue);

    Boolean paramAsBoolean(String key, Boolean defaultValue);
  }

  org.elasticsearch.common.xcontent.ToXContent.Params EMPTY_PARAMS = new org.elasticsearch.common.xcontent.ToXContent.Params() {
    @Override
    public String param(String key) {
      return null;
    }

    @Override
    public String param(String key, String defaultValue) {
      return defaultValue;
    }

    @Override
    public boolean paramAsBoolean(String key, boolean defaultValue) {
      return defaultValue;
    }

    @Override
    public Boolean paramAsBoolean(String key, Boolean defaultValue) {
      return defaultValue;
    }

  };

  class MapParams implements org.elasticsearch.common.xcontent.ToXContent.Params {
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(
        Loggers.getLogger(org.elasticsearch.common.xcontent.ToXContent.MapParams.class));

    private final Map<String, String> params;

    public MapParams(Map<String, String> params) {
      this.params = params;
    }

    @Override
    public String param(String key) {
      return params.get(key);
    }

    @Override
    public String param(String key, String defaultValue) {
      String value = params.get(key);
      if (value == null) {
        return defaultValue;
      }
      return value;
    }

    @Override
    public boolean paramAsBoolean(String key, boolean defaultValue) {
      return paramAsBoolean(key, (Boolean) defaultValue);
    }

    @Override
    public Boolean paramAsBoolean(String key, Boolean defaultValue) {
      String rawParam = param(key);
      if (rawParam != null && Booleans.isStrictlyBoolean(rawParam) == false) {
        DEPRECATION_LOGGER.deprecated("Expected a boolean [true/false] for [{}] but got [{}]", key, rawParam);
      }
      return Booleans.parseBoolean(rawParam, defaultValue);
    }
  }

  class DelegatingMapParams extends org.elasticsearch.common.xcontent.ToXContent.MapParams {

    private final org.elasticsearch.common.xcontent.ToXContent.Params delegate;

    public DelegatingMapParams(Map<String, String> params, org.elasticsearch.common.xcontent.ToXContent.Params delegate) {
      super(params);
      this.delegate = delegate;
    }

    @Override
    public String param(String key) {
      return super.param(key, delegate.param(key));
    }

    @Override
    public String param(String key, String defaultValue) {
      return super.param(key, delegate.param(key, defaultValue));
    }

    @Override
    public boolean paramAsBoolean(String key, boolean defaultValue) {
      return super.paramAsBoolean(key, delegate.paramAsBoolean(key, defaultValue));
    }

    @Override
    public Boolean paramAsBoolean(String key, Boolean defaultValue) {
      return super.paramAsBoolean(key, delegate.paramAsBoolean(key, defaultValue));
    }
  }

  XContentBuilder toXContent(XContentBuilder builder, org.elasticsearch.common.xcontent.ToXContent.Params params) throws IOException;

  default boolean isFragment() {
    return true;
  }
}
