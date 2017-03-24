package com.cds.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 命令行执行脚本，适用于linux和Windows
 */
public class CmdUtils {

    /**
     * 系统类型
     *
     * @see #OS_LINUX
     * @see #OS_Windows
     */
    private static int os;

    private static final int OS_LINUX = 1;

    private static final int OS_Windows = 2;

    public static final String UTF8 = "UTF8";

    public static final String GBK = "GBK";

    static {
        String osName = System.getProperty("os.name");
        if (osName.contains("Windows")) {
            os = OS_Windows;
        } else {
            os = OS_LINUX;
        }
    }

    /**
     * 异步执行(执行后不等待结果返回)
     *
     * @param cmd 控制台命令
     * @throws IOException
     */
    public static Process executeNoWait(String cmd) throws IOException {
        String[] cmds;
        if (OS_Windows == os) {
            cmds = new String[]{"cmd", "/c", cmd};
        } else {
            cmds = new String[]{"/bin/sh", "-c", cmd};
        }
        return Runtime.getRuntime().exec(cmds);
    }

    /**
     * 阻塞执行 (执行后等待结果返回)
     *
     * @param cmd 控制台命令
     * @return 控制台命令输出
     * @throws Exception
     */
    public static String execute(String cmd) throws Exception {
        String charset = OS_LINUX == os ? UTF8 : GBK;
        Process ps = executeNoWait(cmd);
        InputStreamReader isr = new InputStreamReader(ps.getInputStream(),
                charset);
        BufferedReader br = new BufferedReader(isr);
        InputStreamReader err = new InputStreamReader(ps.getErrorStream(),
                charset);
        BufferedReader ebr = new BufferedReader(err);
        StringBuilder resultBuff = new StringBuilder();
        String line;
        ps.waitFor();
        String result;
        while ((line = ebr.readLine()) != null) {
            resultBuff.append(line).append("\n");
        }
        if (org.apache.commons.lang.StringUtils.isNotEmpty(resultBuff.toString())) {
            throw new Exception(resultBuff.toString());
        }
        resultBuff.setLength(0);
        while ((line = br.readLine()) != null) {
            resultBuff.append(line).append("\n");
        }
        result = resultBuff.toString();
        return result;
    }
}
