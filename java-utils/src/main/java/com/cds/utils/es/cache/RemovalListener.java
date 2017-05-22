package com.cds.utils.es.cache;

/**
 * Created by chendongsheng5 on 2017/5/22.
 */
@FunctionalInterface
public interface RemovalListener<K, V> {
  void onRemoval(RemovalNotification<K, V> notification);
}
