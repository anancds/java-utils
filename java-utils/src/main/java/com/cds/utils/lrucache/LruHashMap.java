package com.cds.utils.lrucache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by chendongsheng5 on 2017/6/1.
 */
final class LruHashMap<K, V> extends LinkedHashMap<K, V> {

  private final int capacity;

  public LruHashMap(int capacity) {
    super(capacity, 0.75f, true);
    this.capacity = capacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry entry) {
    return size() > capacity;
  }

}
