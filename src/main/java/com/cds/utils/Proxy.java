package com.cds.utils;

/**
 * Created by cds on 12/31/16 22:09.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

public class Proxy {

    /** 日志记录器 */
    private static final Logger LOG = LoggerFactory.getLogger(Proxy.class);

    /**
     * 存放每个被调用程序的一个可读的描述信息
     */
    Map<String, ProgramDescription> programs;

    /**
     * 默认构造函数
     */
    public Proxy() {
        programs = new TreeMap<String, ProgramDescription>();
    }

    /**
     *
     * 输出类名
     *
     * @param programs 类名map
     */
    private static void printUsage(Map<String, ProgramDescription> programs) {
        LOG.warn("Valid program names are:\r\n");
        for (Map.Entry<String, ProgramDescription> item : programs.entrySet()) {
            LOG.info(" " + item.getKey() + ": "
                    + item.getValue().getDescription() + "\r\n");
        }
    }

    /**
     * 增加一个待运行的类到Map中
     * @param name        你想实例化类的名称
     * @param mainClass   你想要添加到Map中的类
     * @param description 类描述信息
     * @throws Throwable  可能抛出的错误
     */
    public void addClass (String name, @SuppressWarnings("rawtypes") Class mainClass,
                          String description) throws Throwable {
        programs.put(name, new ProgramDescription(mainClass, description));
    }

    /**
     *
     * 对类名进行验证执行main方法
     *
     * @param args 类名
     * @return 0或-1
     * @throws Throwable 调用方法抛出的错误
     */
    public int run(String[] args) throws Throwable {

        /* 确保指定了一个类名 */
        if (0 == args.length) {
            LOG.warn("You must give one argument.");
            printUsage(programs);
            return -1;
        }

        /* 判断给定的类名是否有效 */
        ProgramDescription pgm = programs.get(args[0]);
        if (null == pgm) {
            LOG.warn("Unknown program '" + args[0] + "' chosen.");
            printUsage(programs);
            return -1;
        }

        /* 去掉第一个类名参数 */
        String[] newArgs = new String[args.length - 1];
        for (int i = 1; i < args.length; i++) {
            newArgs[i-1] = args[i];
        }

        /* 执行 */
        pgm.invoke(newArgs);

        return 0;
    }

    /**
     *
     * add description of types here
     */
    private static class ProgramDescription {

        /** 参数 */
        static final Class<?>[] PARAM_TYPES = new Class<?>[] { String[].class };

        /**
         * 为代理的程序创建一个描述
         *
         * @param mainClass
         *            代理程序的主类（即包含main方法的）
         * @param description
         *            代理程序的描述信息
         * @throws NoSuchMethodException
         *             如果在该类中没有找到main方法
         * @throws SecurityException
         *             不可用的反射会报错
         */
        public ProgramDescription(Class<?> mainClass, String description)
                throws NoSuchMethodException, SecurityException {
            this.main = mainClass.getMethod("main", PARAM_TYPES);
            this.description = description;
        }

        /**
         * 使用给定的参数args调用程序main方法
         *
         * @param args
         *            给定的参数列表
         * @throws Throwable
         *             调用方法抛出的错误
         */
        public void invoke(String[] args) throws Throwable {
            try {
                main.invoke(null, new Object[] { args });
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        public String getDescription() {
            return description;
        }

        /**
         * main方法
         */
        private Method main;

        /**
         * 程序描述信息
         */
        private String description;

    }   /* ProgramDescription类定义结束 */

    public static void main(String[] args) {
        Proxy pp = new Proxy();
        try {
            pp.addClass("test", Proxy.class, "test");
            pp.run(args);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
