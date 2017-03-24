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

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;

/**
 *
 */
public class JedisFactory implements PooledObjectFactory<Jedis> {
    private final String host;
    private final int port;
    private final int timeout;
    private final String password;
    private final int database;
    private final String clientName;

    public JedisFactory(final String host, final int port, final int timeout, final String password, final int database) {
        this(host, port, timeout, password, database, null);
    }

    public JedisFactory(final String host, final int port, final int timeout, final String password,
                        final int database, final String clientName) {
        super();
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
    }

    public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception {
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.getDB() != database) {
            jedis.select(database);
        }

    }

    public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception {
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                }
                jedis.disconnect();
            } catch (Exception e) {

            }
        }

    }

    public PooledObject<Jedis> makeObject() throws Exception {
        final Jedis jedis = new Jedis(this.host, this.port, this.timeout);

        jedis.connect();
        if (null != this.password) {
            jedis.auth(this.password);
        }
        if (database != 0) {
            jedis.select(database);
        }
        if (clientName != null) {
            jedis.clientSetname(clientName);
        }

        return new DefaultPooledObject<>(jedis);
    }

    public void passivateObject(PooledObject<Jedis> pooledJedis) throws Exception {
    }

    public boolean validateObject(PooledObject<Jedis> pooledJedis) {
        final BinaryJedis jedis = pooledJedis.getObject();
        try {
            return jedis.isConnected() && jedis.ping().equals("PONG");
        } catch (final Exception e) {
            return false;
        }
    }
}
