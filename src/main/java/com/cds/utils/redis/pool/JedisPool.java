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

import org.apache.commons.pool2.impl.GenericObjectPool;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Pool;

public class JedisPool extends Pool<Jedis> {

    protected String poolName;

    protected HostAndPort address;

    protected ConnectionInfo connectionInfo;

    /**
     * @param maxPoolSize
     * @return
     */
    public static JedisPoolConfig createPoolConfig(int maxPoolSize) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxPoolSize);
        config.setMaxIdle(maxPoolSize);

        config.setTimeBetweenEvictionRunsMillis(600 * 1000);

        return config;
    }

    /**
     * @param address
     * @param connectionInfo
     * @param config
     */
    public void initInternalPool(HostAndPort address, ConnectionInfo connectionInfo,
        JedisPoolConfig config) {
        this.poolName = poolName;
        this.address = address;
        this.connectionInfo = connectionInfo;
        JedisFactory factory =
            new JedisFactory(address.getHost(), address.getPort(), connectionInfo.getTimeout(),
                connectionInfo.getPassword(), connectionInfo.getDatabase());

        internalPool = new GenericObjectPool(factory, config);
    }

    public HostAndPort getAddress() {
        return address;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }
}
