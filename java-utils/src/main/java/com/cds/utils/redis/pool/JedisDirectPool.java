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

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

public class JedisDirectPool extends JedisPool{

    public JedisDirectPool(String poolName, HostAndPort address, JedisPoolConfig config) {
        this(poolName, address, new ConnectionInfo(), config);
    }

    public JedisDirectPool(String poolName, HostAndPort address, ConnectionInfo connectionInfo, JedisPoolConfig config) {
        initInternalPool(address, connectionInfo, config);
        this.poolName = poolName;
    }
}
