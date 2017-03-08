package com.cds.utils.jobscheduler;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public class QuartzJobWrapper implements Job {

    public QuartzJobWrapper() {
        super();
    }

    /**
     * 执行调度
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        JobDetail detail = context.getJobDetail();
        JobDataMap data = detail.getJobDataMap();
        JobSchedulerAgent delegate = (JobSchedulerAgent) data.get("agent");
        delegate.execute();
    }

}
