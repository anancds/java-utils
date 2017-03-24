package com.cds.utils.jobscheduler;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public class JobExecuteRules {

  private String startTime = "";    // 调度计划启动时间，不设置表示立刻启动
  private String stopTime = "";     // 停止调度计划的时间，不设置表示没有这个限制
  private long delay = 0;           // 调度任务延迟启动时间(单位ms)，实际启动时间为starttime + delay
  private int iterations = -1;      // 最大调度次数， -1表示没有限制 ,update，初始值置为1，避免空的时候被解析器用0替换
  private int priority = 0;         // 优先级
  private JobTriggerDescriptor trigger = new JobTriggerDescriptor(); // 一次迭代的触发规则描述

  public JobExecuteRules() {
  }

  public JobExecuteRules(String startTime, String stopTime, long delay, int iterations, int priority, JobTriggerDescriptor trigger) {
    this.startTime = startTime;
    this.stopTime = stopTime;
    this.delay = delay;
    this.iterations = iterations;
    this.priority = priority;
    this.trigger = trigger;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getStopTime() {
    return stopTime;
  }

  public void setStopTime(String stopTime) {
    this.stopTime = stopTime;
  }

  public long getDelay() {
    return delay;
  }

  public void setDelay(long delay) {
    this.delay = delay;
  }

  public int getIterations() {
    return iterations;
  }

  public void setIterations(int iterations) {
    if(iterations != 0){
      this.iterations = iterations;
    }
  }

  public JobTriggerDescriptor getTrigger() {
    return trigger;
  }

  public void setTrigger(JobTriggerDescriptor trigger) {
    this.trigger = trigger;
    if(trigger != null && "SIMPLE".equals(trigger.getType())){
      setIterations(1);
    }
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof JobExecuteRules)) return false;

    JobExecuteRules that = (JobExecuteRules) o;

    if (delay != that.delay) return false;
    if (iterations != that.iterations) return false;
    if (startTime != null ? !startTime.equals(that.startTime) : that.startTime != null) return false;
    if (stopTime != null ? !stopTime.equals(that.stopTime) : that.stopTime != null) return false;
    return trigger != null ? trigger.equals(that.trigger) : that.trigger == null;

  }

  @Override
  public int hashCode() {
    int result = startTime != null ? startTime.hashCode() : 0;
    result = 31 * result + (stopTime != null ? stopTime.hashCode() : 0);
    result = 31 * result + (int) (delay ^ (delay >>> 32));
    result = 31 * result + iterations;
    result = 31 * result + (trigger != null ? trigger.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SchedulerPlanExecuteRules{" +
        "startTime='" + startTime + '\'' +
        ", stopTime='" + stopTime + '\'' +
        ", delay=" + delay +
        ", iterations=" + iterations +
        ", trigger=" + trigger +
        '}';
  }
}
