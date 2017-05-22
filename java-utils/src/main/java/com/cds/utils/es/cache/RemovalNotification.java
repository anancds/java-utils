package com.cds.utils.es.cache;

/**
 * Created by chendongsheng5 on 2017/5/22.
 */
public class RemovalNotification<K, V> {

  private final K key;
  private final V value;
  private final RemovalReason removalReason;

  public RemovalNotification(K key, V value, RemovalReason removalReason) {
    this.key = key;
    this.value = value;
    this.removalReason = removalReason;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public RemovalReason getRemovalReason() {
    return removalReason;
  }

  public enum RemovalReason {REPLACED, INVALIDATED, EVICTED}
}
