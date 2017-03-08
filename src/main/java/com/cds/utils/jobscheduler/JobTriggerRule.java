package com.cds.utils.jobscheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by chendongsheng5 on 2017/3/8.
 */
public abstract class JobTriggerRule {

    public static SimpleTriggerRule SIMPLE_TRIGGER = new SimpleTriggerRule();
    public static FixRateTriggerRule FIX_RATE_TRIGGER = new FixRateTriggerRule();
    public static CronTriggerRule CRON_TRIGGER = new CronTriggerRule();

    /**
     * 获取触发器类型
     *
     */
    public abstract String getType();

    /**
     * 获取触发器参数名
     *
     */
    public abstract Set<String> getArguments();

    public static class SimpleTriggerRule extends JobTriggerRule {

        /**
         * 获取触发器类型
         *
         */
        @Override
        public String getType() {
            return "SIMPLE";
        }

        /**
         * 获取触发器参数名
         *
         */
        @Override
        public Set<String> getArguments() {
            return new HashSet<>();
        }

        public JobTriggerDescriptor create() {
            Map<String, String> args = new HashMap<>();
            return new JobTriggerDescriptor(getType(), args);
        }
    }

    /**
     *  固定频率，每隔多少时间触发一次
     */
    public static class FixRateTriggerRule extends JobTriggerRule {
        public final Arguments ARGUMENTS = new Arguments();

        private FixRateTriggerRule() {
        }

        /**
         * 获取触发器类型
         *
         */
        @Override
        public String getType() {
            return "FIX_RATE";
        }

        /**
         * 获取触发器参数名
         *
         */
        @Override
        public Set<String> getArguments() {
            Set<String> arguments = new HashSet<>();
            arguments.add(ARGUMENTS.INTERVAL);
            return arguments;
        }

        public JobTriggerDescriptor create(int interval) {
            Map<String, String> args = new HashMap<>();
            args.put("interval", interval + "");
            return new JobTriggerDescriptor(getType(), args);
        }

        public static class Arguments {
            public final String INTERVAL = "interval"; // 间隔时间，秒
        }
    }

    /**
     * cron表达式触发器
     */
    public static class CronTriggerRule extends JobTriggerRule {
        public final Arguments ARGUMENTS = new Arguments();
        private CronTriggerRule() {}

        /**
         * 获取触发器类型
         *
         */
        @Override
        public String getType() {
            return "CRON";
        }

        /**
         * 获取触发器参数名
         *
         */
        @Override
        public Set<String> getArguments() {
            Set<String> arguments = new HashSet<>();
            arguments.add("expression"); // cron表达式
            return arguments;
        }

        public JobTriggerDescriptor create(String expression) {
            Map<String, String> args = new HashMap<>();
            args.put("expression", expression);
            return new JobTriggerDescriptor(getType(), args);
        }

        public static class Arguments {
            public final static String EXPRESSION = "expression";
        }
    }
}
