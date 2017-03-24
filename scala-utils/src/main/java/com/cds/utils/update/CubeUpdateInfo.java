package com.cds.utils.update;

import java.io.IOException;
import java.util.Map;
import org.springframework.util.StringUtils;

/**
 * Created by chendongsheng5 on 2017/3/24.
 */
public class CubeUpdateInfo {

  private String cubeName;

  // 定时更新cron表达式
  public final static String CRON_EXPRESSION_PARAM = "cronExpression";
  private String cornExpression;

  public CubeUpdateInfo(String cubeName, Map<String, String> parameters) throws IOException {

    this.cubeName = cubeName;
    parametersParse(parameters);
  }

  private void parametersParse(Map<String, String> parameters) throws IOException {

    if (!parameters.containsKey(CRON_EXPRESSION_PARAM) || !StringUtils.hasText(parameters.get(CRON_EXPRESSION_PARAM)))
      throw new IOException("The parameter '" + CRON_EXPRESSION_PARAM + "' must be provide.");
    this.cornExpression = parameters.get(CRON_EXPRESSION_PARAM);
  }

  public String getCubeName() {
    return cubeName;
  }

  public String getCornExpression() {
    return cornExpression;
  }
}
