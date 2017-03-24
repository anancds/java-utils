package com.cds.utils.redis;

import com.cds.utils.redis.pool.JedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Jedis类库
 */
public class JedisUtils {

    private static Logger logger = LoggerFactory.getLogger(JedisUtils.class);
    private static final String OK_CODE = "OK";

    private static final String OK_MULTI_CODE = "+OK";

    /**
     * 判断redis返回值
     */
    public static boolean isStatusOk(String status) {
        return (null != status) && (OK_CODE.equals(status) || OK_MULTI_CODE.equals(status));
    }

    /**
     * 强制销毁redis
     */
    public static void destroyJedis(Jedis jedis) {
        if ((jedis != null) && jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                    logger.error("jedis quit failed!");
                }
                jedis.disconnect();
            } catch (Exception e) {
                logger.error("jedis disconnect failed!");
            }
        }
    }

    public static boolean ping(JedisPool pool) {
        JedisTemplate template = new JedisTemplate(pool);

        try {
            String result =
                    template.execute(BinaryJedis::ping);
            return (result != null) && result.equals("PONG");
        } catch (JedisException e) {
            logger.error("jedis ping failed!");
            return false;
        }

    }

}
