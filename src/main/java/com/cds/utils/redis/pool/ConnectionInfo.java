/**
 * Copyright (c) 2015, zhejiang Unview Technologies Co., Ltd.
 * All rights reserved.
 * <http://www.uniview.com/>
 * -----------------------------------------------------------
 * Product      :BigData
 * Module Name  :
 * Project Name :BigdataModules
 * Package Name :com.uniview.modules.nosql.redis.pool
 * Date Created :2015/10/26
 * Creator      :c02132
 * Description  :
 * -----------------------------------------------------------
 * Modification History
 * Date        Name          Description
 * ------------------------------------------------------------
 * 2015/10/26      c02132         BigData project,new code file.
 * ------------------------------------------------------------
 */
package com.cds.utils.redis.pool;

import redis.clients.jedis.Protocol;

/**
 * redis connect information
 */
@SuppressWarnings("unused")
public class ConnectionInfo {

    public static final String DEFAULT_PASSWORD = null;

    private int database = Protocol.DEFAULT_DATABASE;
    private String password = DEFAULT_PASSWORD;
    private int timeout = Protocol.DEFAULT_TIMEOUT;

    public ConnectionInfo() {
    }

    public ConnectionInfo(int database, String password, int timeout) {
        this.timeout = timeout;
        this.password = password;
        this.database = database;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "ConnectionInfo [database=" + database + ", password=" + password + ", timeout=" + timeout + "]";
    }
}
