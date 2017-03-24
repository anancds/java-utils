package com.cds.utils.redis;

import com.cds.utils.redis.pool.JedisPool;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Jedis wrap base on redis 3.0
 */
@SuppressWarnings("unused")
public class JedisTemplate {

    private static Logger logger = LoggerFactory.getLogger(JedisTemplate.class);

    private JedisPool jedisPool;

    public JedisTemplate(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * get jedisPool
     *
     * @return jedisPool
     */
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    /**
     * Jedis action has return
     *
     * @param <T>
     */
    public interface JedisAction<T> {
        T action(Jedis jedis);
    }


    /**
     * Jedis action without return
     */
    public interface JedisActionNoResult {
        void action(Jedis jedis);
    }


    /**
     * Pipeline action has return
     */
    public interface PipelineAction {
        List<Object> action(Pipeline Pipeline);
    }


    /**
     * Pipeline action without return
     */
    public interface PipelineActionNoResult {
        void action(Pipeline Pipeline);
    }


    public <T> T execute(JedisAction<T> jedisAction) throws JedisException {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = jedisPool.getResource();
            return jedisAction.action(jedis);
        } catch (JedisException e) {
            broken = handleJedisException(e);
            throw e;
        } finally {
            closeResource(jedis, broken);
        }
    }

    public void execute(JedisActionNoResult jedisAction) throws JedisException {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = jedisPool.getResource();
            jedisAction.action(jedis);
        } catch (JedisException e) {
            broken = handleJedisException(e);
            throw e;
        } finally {
            closeResource(jedis, broken);
        }
    }

    public List<Object> execute(PipelineAction pipelineAction) throws JedisException {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            pipelineAction.action(pipeline);
            return pipeline.syncAndReturnAll();
        } catch (JedisException e) {
            broken = handleJedisException(e);
            throw e;
        } finally {
            closeResource(jedis, broken);
        }
    }

    public void execute(PipelineActionNoResult pipelineAction) throws JedisException {
        Jedis jedis = null;
        boolean broken = false;
        try {
            jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            pipelineAction.action(pipeline);
            pipeline.sync();
        } catch (JedisException e) {
            broken = handleJedisException(e);
            throw e;
        } finally {
            closeResource(jedis, broken);
        }
    }

    /**
     * handle redis exception
     *
     * @param jedisException jedis exception
     */
    protected boolean handleJedisException(JedisException jedisException) {
        if (jedisException instanceof JedisConnectionException) {
            logger.error("Redis connection " + jedisPool.getAddress() + " lost.", jedisException);
        } else if (jedisException instanceof JedisDataException) {
            if ((jedisException.getMessage() != null) && (jedisException.getMessage().indexOf("READONLY") != -1)) {
                logger.error("Redis connection " + jedisPool.getAddress() + " are read-only slave.",
                        jedisException);
            } else {
                return false;
            }
        } else {
            logger.error("Jedis exception happen.", jedisException);
        }
        return true;
    }

    /**
     * close redis
     */
    protected void closeResource(Jedis jedis, boolean connectionBroken) {
        try {
            if (connectionBroken) {
                jedisPool.returnBrokenResource(jedis);
            } else {
                jedisPool.returnResource(jedis);
            }

        } catch (Exception e) {
            logger.error("close jedis connection failed, will fore close the jedis.", e);
            JedisUtils.destroyJedis(jedis);
        }

    }

    //************************* Common Actions **********************************

    public Set<String> keys(final String pattern) {
        return execute((JedisAction<Set<String>>) jedis -> jedis.keys(pattern));
    }


    public Boolean del(final String... keys) {
        return execute((JedisAction<Boolean>) jedis -> jedis.del(keys) == keys.length);
    }

    public void flushDB() {
        execute((JedisActionNoResult) BinaryJedis::flushDB);
    }

    /**
     * @see Jedis#expire(String, int)
     */
    public Long expire(final String key, int seconds) {
        return execute((JedisAction<Long>) jedis -> jedis.expire(key, seconds));
    }

    /**
     * @see Jedis#expireAt(String, long)
     */
    public Long expireAt(final String key, long unixTime) {
        return execute((JedisAction<Long>) jedis -> jedis.expireAt(key, unixTime));
    }

    public String select(final int key) {
        return execute((JedisAction<String>) jedis -> jedis.select(key));
    }


    //************************** String Actions **************************************

    /**
     * get value by key
     */
    public String get(final String key) {
        return execute((JedisAction<String>) jedis -> jedis.get(key));
    }

    /**
     * get value by key
     */
    public Long getAsLong(final String key) {
        String result = get(key);
        return result != null ? Long.valueOf(result) : null;
    }

    /**
     * get value by key
     */
    public Integer getAsInt(final String key) {
        String result = get(key);
        return result != null ? Integer.valueOf(result) : null;
    }


    /**
     */
    public List<String> mget(final String... keys) {
        return execute((JedisAction<List<String>>) jedis -> jedis.mget(keys));
    }

    public void set(final String key, final String value) {
        execute((JedisActionNoResult) jedis -> jedis.set(key, value));
    }


    public void setex(final String key, final String value, final int seconds) {
        execute((JedisActionNoResult) jedis -> jedis.setex(key, seconds, value));
    }


    public Boolean setnx(final String key, final String value) {
        return execute((JedisAction<Boolean>) jedis -> jedis.setnx(key, value) == 1);
    }


    public Boolean setnxex(final String key, final String value, final int seconds) {
        return execute((JedisAction<Boolean>) jedis -> {
            String result = jedis.set(key, value, "NX", "EX", seconds);
            return JedisUtils.isStatusOk(result);
        });
    }


    public String getSet(final String key, final String value) {
        return execute((JedisAction<String>) jedis -> jedis.getSet(key, value));
    }


    public Long incr(final String key) {
        return execute((JedisAction<Long>) jedis -> jedis.incr(key));
    }

    public Long incrBy(final String key, final long increment) {
        return execute((JedisAction<Long>) jedis -> jedis.incrBy(key, increment));
    }

    public Double incrByFloat(final String key, final double increment) {
        return execute((JedisAction<Double>) jedis -> jedis.incrByFloat(key, increment));
    }

    public Long decr(final String key) {
        return execute((JedisAction<Long>) jedis -> jedis.decr(key));
    }

    public Long decrBy(final String key, final long decrement) {
        return execute((JedisAction<Long>) jedis -> jedis.decrBy(key, decrement));
    }

    //-----------------------------  Hash Actions ------------------------

    public String hget(final String key, final String fieldName) {
        return execute((JedisAction<String>) jedis -> jedis.hget(key, fieldName));
    }

    public List<String> hmget(final String key, final String... fieldsNames) {
        return execute((JedisAction<List<String>>) jedis -> jedis.hmget(key, fieldsNames));
    }

    public Map<String, String> hgetAll(final String key) {
        return execute((JedisAction<Map<String, String>>) jedis -> jedis.hgetAll(key));
    }

    public void hset(final String key, final String fieldName, final String value) {
        execute((JedisActionNoResult) jedis -> jedis.hset(key, fieldName, value));
    }

    public void hmset(final String key, final Map<String, String> map) {
        execute((JedisActionNoResult) jedis -> jedis.hmset(key, map));

    }

    public Boolean hsetnx(final String key, final String fieldName, final String value) {
        return execute((JedisAction<Boolean>) jedis -> jedis.hsetnx(key, fieldName, value) == 1);
    }

    public Long hincrBy(final String key, final String fieldName, final long increment) {
        return execute((JedisAction<Long>) jedis -> jedis.hincrBy(key, fieldName, increment));
    }

    public Double hincrByFloat(final String key, final String fieldName, final double increment) {
        return execute(
                (JedisAction<Double>) jedis -> jedis.hincrByFloat(key, fieldName, increment));
    }

    public Long hdel(final String key, final String... fieldsNames) {
        return execute((JedisAction<Long>) jedis -> jedis.hdel(key, fieldsNames));
    }

    public Boolean hexists(final String key, final String fieldName) {
        return execute((JedisAction<Boolean>) jedis -> jedis.hexists(key, fieldName));
    }

    public Boolean exists(final String key) {
        return execute((JedisAction<Boolean>) jedis -> jedis.exists(key));
    }

    public Set<String> hkeys(final String key) {
        return execute((JedisAction<Set<String>>) jedis -> jedis.hkeys(key));
    }

    public Long hlen(final String key) {
        return execute((JedisAction<Long>) jedis -> jedis.hlen(key));
    }

    // / List Actions ///

    public Long lpush(final String key, final String... values) {
        return execute((JedisAction<Long>) jedis -> jedis.lpush(key, values));
    }

    public String rpop(final String key) {
        return execute((JedisAction<String>) jedis -> jedis.rpop(key));
    }

    public String brpop(final String key) {
        return execute((JedisAction<String>) jedis -> {
            List<String> nameValuePair = jedis.brpop(new String[]{key});
            if (nameValuePair != null) {
                return nameValuePair.get(1);
            } else {
                return null;
            }
        });
    }

    public String brpop(final int timeout, final String key) {
        return execute((JedisAction<String>) jedis -> {
            List<String> nameValuePair = jedis.brpop(timeout, key);
            if (nameValuePair != null) {
                return nameValuePair.get(1);
            } else {
                return null;
            }
        });
    }

    public String rpoplpush(final String sourceKey, final String destinationKey) {
        return execute((JedisAction<String>) jedis -> jedis.rpoplpush(sourceKey, destinationKey));
    }

    public String brpoplpush(final String source, final String destination, final int timeout) {
        return execute(
                (JedisAction<String>) jedis -> jedis.brpoplpush(source, destination, timeout));
    }

    public Long llen(final String key) {
        return execute((JedisAction<Long>) jedis -> jedis.llen(key));
    }

    public String lindex(final String key, final long index) {
        return execute((JedisAction<String>) jedis -> jedis.lindex(key, index));
    }

    public List<String> lrange(final String key, final int start, final int end) {
        return execute((JedisAction<List<String>>) jedis -> jedis.lrange(key, start, end));
    }

    public void ltrim(final String key, final int start, final int end) {
        execute((JedisActionNoResult) jedis -> jedis.ltrim(key, start, end));
    }

    public void ltrimFromLeft(final String key, final int size) {
        execute((JedisActionNoResult) jedis -> jedis.ltrim(key, 0, size - 1));
    }

    public Boolean lremFirst(final String key, final String value) {
        return execute((JedisAction<Boolean>) jedis -> {
            Long count = jedis.lrem(key, 1, value);
            return (count == 1);
        });
    }

    public Boolean lremAll(final String key, final String value) {
        return execute((JedisAction<Boolean>) jedis -> {
            Long count = jedis.lrem(key, 0, value);
            return (count > 0);
        });
    }

    //---------------- Set Actions --------------------------------
    public Boolean sadd(final String key, final String member) {
        return execute((JedisAction<Boolean>) jedis -> jedis.sadd(key, member) == 1);
    }

    public Set<String> smembers(final String key) {
        return execute((JedisAction<Set<String>>) jedis -> jedis.smembers(key));
    }

    // / Ordered Set Actions ///
    public Boolean zadd(final String key, final double score, final String member) {
        return execute((JedisAction<Boolean>) jedis -> jedis.zadd(key, score, member) == 1);
    }

    public Double zscore(final String key, final String member) {
        return execute((JedisAction<Double>) jedis -> jedis.zscore(key, member));
    }

    public Long zrank(final String key, final String member) {
        return execute((JedisAction<Long>) jedis -> jedis.zrank(key, member));
    }

    public Long zrevrank(final String key, final String member) {
        return execute((JedisAction<Long>) jedis -> jedis.zrevrank(key, member));
    }

    public Long zcount(final String key, final double min, final double max) {
        return execute((JedisAction<Long>) jedis -> jedis.zcount(key, min, max));
    }

    public Set<String> zrange(final String key, final int start, final int end) {
        return execute((JedisAction<Set<String>>) jedis -> jedis.zrange(key, start, end));
    }

    public Set<Tuple> zrangeWithScores(final String key, final int start, final int end) {
        return execute((JedisAction<Set<Tuple>>) jedis -> jedis.zrangeWithScores(key, start, end));
    }

    public Set<String> zrevrange(final String key, final int start, final int end) {
        return execute((JedisAction<Set<String>>) jedis -> jedis.zrevrange(key, start, end));
    }

    public Set<Tuple> zrevrangeWithScores(final String key, final int start, final int end) {
        return execute(
                (JedisAction<Set<Tuple>>) jedis -> jedis.zrevrangeWithScores(key, start, end));
    }

    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return execute((JedisAction<Set<String>>) jedis -> jedis.zrangeByScore(key, min, max));
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min,
                                              final double max) {
        return execute(
                (JedisAction<Set<Tuple>>) jedis -> jedis.zrangeByScoreWithScores(key, min, max));
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return execute((JedisAction<Set<String>>) jedis -> jedis.zrevrangeByScore(key, max, min));
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max,
                                                 final double min) {
        return execute(
                (JedisAction<Set<Tuple>>) jedis -> jedis.zrevrangeByScoreWithScores(key, max, min));
    }

    public Boolean zrem(final String key, final String member) {
        return execute((JedisAction<Boolean>) jedis -> jedis.zrem(key, member) == 1);
    }

    public Long zremByScore(final String key, final double start, final double end) {
        return execute((JedisAction<Long>) jedis -> jedis.zremrangeByScore(key, start, end));
    }

    public Long zremByRank(final String key, final long start, final long end) {
        return execute((JedisAction<Long>) jedis -> jedis.zremrangeByRank(key, start, end));
    }

    public Long zcard(final String key) {
        return execute((JedisAction<Long>) jedis -> jedis.zcard(key));
    }

    //--------------------------sub/pub------------------------------------------

    public void subscribe(final JedisPubSub jedisPubSub, final String... channels) {
        execute((JedisActionNoResult) jedis -> jedis.subscribe(jedisPubSub, channels));
    }

    public Long publish(final String channel, final String message) {
        return execute((JedisAction<Long>) jedis -> jedis.publish(channel, message));
    }
    //---------------------------GEO-----------------------------------


}
