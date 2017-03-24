package com.cds.utils.jobscheduler;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public class JobPlanDescriptor {

  private String id = "";             // 作业ID
  private String submitTime = "";     // 提交时间，格式"yyyy-MM-ddTHH:mm:ssZ"
  private String description = "";    // 调度计划描述信息
  private JobExecuteRules rules = new JobExecuteRules(); // 执行规则，包括启动和停止
  private long startupTimeout = -1; // 启动超时时间，单位秒，默认没有超时


  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(String submitTime) {
    this.submitTime = submitTime;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public JobExecuteRules getRules() {
    return rules;
  }

  public void setRules(JobExecuteRules rules) {
    this.rules = rules;
  }


  public long getStartupTimeout() {
    return startupTimeout;
  }

  public void setStartupTimeout(long startupTimeout) {
    this.startupTimeout = startupTimeout;
  }

}
