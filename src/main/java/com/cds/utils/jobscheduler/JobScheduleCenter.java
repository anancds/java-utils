package com.cds.utils.jobscheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public class JobScheduleCenter {

    private static final Logger LOG = LoggerFactory.getLogger(JobScheduleCenter.class);

    private Status status = Status.UNINITIALIZED;
    private Scheduler scheduler;

    // 作业调度器代理队列,<jobID, Agent实例>
    Map<String, JobSchedulerAgent> maps = new HashMap<>();

    private JobScheduleCenter() {

    }

    private static class SingletonHolder{
        private final static  JobScheduleCenter INSTANCE = new JobScheduleCenter();
    }

    public static JobScheduleCenter getInstance(){

        return SingletonHolder.INSTANCE;
    }

    public synchronized void initialize(Properties props) throws SchedulerException {

        if (Status.UNINITIALIZED == this.status) {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory(props);
            this.scheduler = schedulerFactory.getScheduler();
            this.scheduler.start();
            this.status = Status.INITIALIZED;
            LOG.info("The Job Scheduler Center is initialized.");
        }
    }

    public synchronized void uninitialize() throws SchedulerException {

        if (Status.INITIALIZED == this.status) {
            this.scheduler.shutdown();
            this.status = Status.UNINITIALIZED;
            LOG.info("The Job Scheduler Center is uninitialized.");
        }
    }

    public synchronized void scheduleJob(JobSchedulerAgent jobSchedulerAgent) throws SchedulerException {

        if (null == jobSchedulerAgent)
            throw new SchedulerException("The job agent is null.", new NullPointerException());
        String jobId = jobSchedulerAgent.getJobPlan().getId();
        if (maps.containsKey(jobId))
            throw new SchedulerException("The job already scheduled.");

        jobSchedulerAgent.setScheduler(this.scheduler);
        jobSchedulerAgent.schedule();

        maps.put(jobId, jobSchedulerAgent);
    }

    public synchronized void stopJob(String jobID) throws SchedulerException {

        if (!maps.containsKey(jobID))
            throw new SchedulerException("The job scheduler is not existed.");
        maps.get(jobID).stop();
    }

    public synchronized void stopAllJobs() throws SchedulerException {

        for (JobSchedulerAgent agent : maps.values()) {
            agent.stop();
        }
    }

    public synchronized void deleteJob(String jobID) throws SchedulerException {

        if (!maps.containsKey(jobID))
            throw new SchedulerException("The job scheduler is not existed.");
        // 删除之前先停止
        stopJob(jobID);
        maps.remove(jobID).delete();
    }

    public synchronized void deleteAllJobs() throws SchedulerException {

        for (JobSchedulerAgent agent : maps.values()) {
            agent.delete();
        }
        maps.clear();
    }

    public enum Status {

        INVALIDATE,    // 非法的
        INITIALIZED,   // 初始化的
        UNINITIALIZED; // 未初始化的

        public static Status fromString(String status) {

            try {
                return valueOf(status);
            } catch (Throwable t) {
                return INVALIDATE;
            }
        }
    }


}
