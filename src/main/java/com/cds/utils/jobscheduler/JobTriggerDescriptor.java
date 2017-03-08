package com.cds.utils.jobscheduler;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public class JobTriggerDescriptor {

    private String type = JobTriggerRule.CRON_TRIGGER.getType();  // 触发器类型
    private Map<String, String> arguments = new HashMap<>(); // 触发器参数

    public JobTriggerDescriptor() {
    }

    public JobTriggerDescriptor(String type, Map<String, String> arguments) {
        this.arguments = arguments;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, String> arguments) {
        this.arguments = arguments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobTriggerDescriptor)) return false;

        JobTriggerDescriptor that = (JobTriggerDescriptor) o;

        return !(type != null ? !type.equals(that.type) : that.type != null) && (arguments != null ? arguments.equals(that.arguments) : that
                .arguments == null);

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchedulerPlanTriggerDescriptor{" +
                "type='" + type + '\'' +
                ", arguments=" + arguments +
                '}';
    }
}
