package com.cds.utils.es.cache;

/**
 * Created by chendongsheng5 on 2017/5/22.
 */
@FunctionalInterface
public interface CacheLoader<K, V> {
  V load(K key) throws Exception;
}
