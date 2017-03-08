package com.cds.utils.jobscheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.CronExpression;
import org.quartz.SchedulerException;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public class TestJobSchedulerAgent {

  @Test
  public void test() throws SchedulerException, InterruptedException {

    JobPlanDescriptor job = new JobPlanDescriptor();
    job.setId("job1");

    JobExecuteRules jobExecuteRules = new JobExecuteRules();

    Map<String, String> arguments = new HashMap<>(1);
    arguments.put(JobTriggerRule.CRON_TRIGGER.ARGUMENTS.EXPRESSION, "*/2 * * * * ?");
    JobTriggerDescriptor jobTriggerDescriptor = new JobTriggerDescriptor();
    jobTriggerDescriptor.setArguments(arguments);

    jobExecuteRules.setTrigger(jobTriggerDescriptor);

    job.setRules(jobExecuteRules);

    JobScheduleCenter jobScheduleCenter = JobScheduleCenter.getInstance();
    Properties props = new Properties();
    props.setProperty("org.quartz.threadPool.threadCount", "10");
    jobScheduleCenter.initialize(props);

    System.out.println("current time is: " + System.currentTimeMillis());

    jobScheduleCenter.scheduleJob(new JobSchedulerAgentTest(job));

    Thread.sleep(10000);

    jobScheduleCenter.deleteAllJobs();

    jobScheduleCenter.uninitialize();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExp() {

    Assert.assertTrue(CronExpression.isValidExpression("*/2 * * * * ?"));
    Assert.assertFalse(CronExpression.isValidExpression("& * * * * ?"));
    Assert.assertFalse(CronExpression.isValidExpression("*/2  * * * ?"));
    Assert.assertFalse(CronExpression.isValidExpression("*/2 * * * * "));
    Assert.assertFalse(CronExpression.isValidExpression(""));
    CronExpression.isValidExpression(null);
  }
}
