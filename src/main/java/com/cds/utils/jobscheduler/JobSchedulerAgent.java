package com.cds.utils.jobscheduler;

import com.cds.utils.DateUtils;
import com.cds.utils.StringUtils;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public abstract class JobSchedulerAgent {

    protected static Logger LOG = LoggerFactory.getLogger(JobSchedulerAgent.class);

    private Scheduler scheduler;
    protected JobPlanDescriptor jobPlan;
    protected SchedulerStatus status = SchedulerStatus.INACTIVE;

    private JobDetail jobDetail;
    private Set<Trigger> triggers;

    public JobSchedulerAgent(JobPlanDescriptor plan) {

        this.jobPlan = plan;
        this.jobDetail = createJobDetail(QuartzJobWrapper.class);
        this.triggers = createJobTriggers();
    }

    public void setScheduler(Scheduler scheduler) {

        this.scheduler = scheduler;
    }

    public JobPlanDescriptor getJobPlan() throws SchedulerException {

        if (null == jobPlan)
            throw new SchedulerException("The job jobPlan descriptor is null.", new NullPointerException());
        return jobPlan;
    }

    /**
     * 根据SchedulerPlanDescriptor创建JobDetail对象
     */
    protected JobDetail createJobDetail(Class<? extends Job> executorClass) {

        Map<String, Object> map = new HashMap<>();
        map.put("agent", this);
        JobDataMap data = new JobDataMap(map);
        return JobBuilder.newJob()
                .ofType(executorClass)
                .withIdentity(this.jobPlan.getId())
                .withDescription(this.jobPlan.getDescription())
                .setJobData(data)
                .build();
    }

    /**
     * 创建作业触发器
     */
    protected Set<Trigger> createJobTriggers() {

        Set<Trigger> triggers = new HashSet<>();
        try {
            JobExecuteRules rules = jobPlan.getRules();
            JobTriggerDescriptor triggerDescription = rules.getTrigger();
            if (JobTriggerRule.SIMPLE_TRIGGER.getType().equals(triggerDescription.getType())) {
                triggers.add(buildSimpleTrigger(jobPlan.getId(), rules));
            } else if (JobTriggerRule.FIX_RATE_TRIGGER.getType().equals(triggerDescription.getType())) {
                triggers.add(buildFixRateTrigger(jobPlan.getId(), rules));
            } else if (JobTriggerRule.CRON_TRIGGER.getType().equals(triggerDescription.getType())) {
                triggers.add(buildCronTrigger(jobPlan.getId(), rules));
            }
        } catch (Throwable t) {
            LOG.warn("Failed to build triggers, jobPlan '{}' will not be carried on.. ", jobPlan.getId(), t);
            t.printStackTrace();
        }
        return triggers;
    }

    /**
     * 创建Simple触发器
     */
    protected Trigger buildSimpleTrigger(String id, JobExecuteRules rules) {

        TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.<SimpleTrigger>newTrigger();
        triggerBuilder.withIdentity(makeTriggerName(id));
        triggerBuilder.withPriority(rules.getPriority());
        Date startDate = new Date();
        String startTime = rules.getStartTime();
        if (StringUtils.hasText(startTime)) {
            startDate = DateUtils.string2Date(startTime);
        }
        triggerBuilder.startAt(startDate);
        return triggerBuilder.build();
    }


    /**
     * 创建Cron触发器
     */
    protected Trigger buildCronTrigger(String id, JobExecuteRules rules) throws InterruptedException {

        JobTriggerDescriptor triggerDescription = rules.getTrigger();
        TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.<CronTrigger>newTrigger();
        triggerBuilder.withIdentity(makeTriggerName(id));
        triggerBuilder.withPriority(rules.getPriority());
        Date startDate = new Date();
        String startTime = rules.getStartTime();
        if (StringUtils.hasText(startTime)) {
            startDate = DateUtils.string2Date(startTime);
        }
        long delayMs = rules.getDelay();
        if (delayMs > 0) {
            startDate = DateUtils.addDay(startDate, delayMs);
        }
        triggerBuilder.startAt(startDate);
        String endTime = rules.getStopTime();
        if (StringUtils.hasText(endTime)) {
            Date end = DateUtils.string2Date(endTime);
            if (end.getTime() < System.currentTimeMillis()) {
                throw new InterruptedException("stop time is too close");
            }
            triggerBuilder.endAt(end);
        }
        // 使用cron调度器，最大迭代次数这样的调度参数就失效了
        String cronExpression = triggerDescription.getArguments().get(JobTriggerRule.CRON_TRIGGER.ARGUMENTS.EXPRESSION);
        triggerBuilder.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).inTimeZone(DateUtils.TIMEZONE_ASIA_SHANGHAI));
        return triggerBuilder.build();
    }

    /**
     * 创建固定频率触发器
     */
    protected Trigger buildFixRateTrigger(String id, JobExecuteRules rules) throws InterruptedException {

        JobTriggerDescriptor triggerDescription = rules.getTrigger();
        TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.<SimpleTrigger>newTrigger();
        triggerBuilder.withIdentity(makeTriggerName(id));
        triggerBuilder.withPriority(rules.getPriority());
        Date startDate = new Date();
        String startTime = rules.getStartTime();
        if (StringUtils.hasText(startTime)) {
            startDate = DateUtils.string2Date(startTime);
        }
        long delayMs = rules.getDelay();
        if (delayMs > 0) {
            startDate = DateUtils.addDay(startDate, delayMs);
        }
        triggerBuilder.startAt(startDate);
        String endTime = rules.getStopTime();
        if (StringUtils.hasText(endTime)) {
            Date end = DateUtils.string2Date(endTime);
            if (end.getTime() < System.currentTimeMillis()) {
                throw new InterruptedException("stop time is too close");
            }
            triggerBuilder.endAt(end);
        }
        int maxIterations = rules.getIterations();
        String intervalSecondsStr = triggerDescription.getArguments().get(JobTriggerRule.FIX_RATE_TRIGGER.ARGUMENTS.INTERVAL);
        int intervalSeconds = !StringUtils.hasText(intervalSecondsStr) ? 0 : Integer.parseInt(intervalSecondsStr);
        if (maxIterations > 0) {
            if (intervalSeconds > 0) {
                triggerBuilder.withSchedule(SimpleScheduleBuilder.repeatSecondlyForTotalCount(maxIterations, intervalSeconds));
            } else {
                triggerBuilder.withSchedule(SimpleScheduleBuilder.repeatSecondlyForTotalCount(maxIterations));
            }
        } else {
            if (intervalSeconds > 0) {
                triggerBuilder.withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(intervalSeconds));
            } else {
                triggerBuilder.withSchedule(SimpleScheduleBuilder.repeatSecondlyForever());
            }
        }
        return triggerBuilder.build();
    }


    /**
     * 创建触发器名称
     */
    private String makeTriggerName(String jobId) {
        return "TRIGGER-" + jobId;
    }

    /**
     * 启动调度计划
     */
    public void schedule() throws SchedulerException {

        String jobId = this.jobPlan.getId();
        try {
            this.scheduler.scheduleJob(this.jobDetail, this.triggers, false);
            this.status = SchedulerStatus.ACTIVE;
            LOG.info("Succeed to schedule job plan '{}', status={}.", jobId, status);
        } catch (SchedulerException e) {
            throw new SchedulerException("Failed to scheduler job plan '" + jobId + "'.", e);
        }
    }

    /**
     * 删除调度计划
     */
    public void delete() throws SchedulerException {

        try {
            // 从调度器中删除
            this.scheduler.deleteJob(this.jobDetail.getKey());
            LOG.info("Succeed to delete job plan '{}', status={}.", this.jobPlan.getId());
            this.status = SchedulerStatus.INACTIVE;
        } catch (SchedulerException e) {
            throw new SchedulerException("Failed to delete job plan '" + this.jobPlan.getId() + "'.", e);
        }
    }


    /**
     * 作业执行体
     */
    public abstract void execute () throws JobExecutionException;

    /**
     * 终止作业
     */
    public abstract void stop();


    /**
     * 暂停作业
     */
    public abstract void pause();

    /**
     * 恢复作业
     */
    public abstract void resume();


    public enum SchedulerStatus {

        INVALIDATE, // 非法的状态
        INACTIVE,    // 未激活，还没有触发
        ACTIVE,     // 已经触发，正在执行
        SUSPENDED,  // 已经触发，暂停中
        FAILED,     // 参数错误，无法启动
        COMPLETED;

        public static SchedulerStatus fromString(String status) {
            try {
                return valueOf(status);
            } catch (Throwable t) {
                // No-ops
            }
            return INVALIDATE;
        }
    }

}
