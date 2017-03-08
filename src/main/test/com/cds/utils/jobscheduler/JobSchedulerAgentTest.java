package com.cds.utils.jobscheduler;

import static org.junit.Assert.*;

import org.junit.Test;
import org.quartz.JobExecutionException;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public class JobSchedulerAgentTest extends JobSchedulerAgent{

  public JobSchedulerAgentTest(JobPlanDescriptor plan) {
    super(plan);
  }

  @Override
  public void execute() throws JobExecutionException {

    System.out.println("i am " + this.jobPlan.getId() + ", running: " + System.currentTimeMillis());
  }

  @Override
  public void stop() {

  }

  @Override
  public void pause() {

  }

  @Override
  public void resume() {

  }
}