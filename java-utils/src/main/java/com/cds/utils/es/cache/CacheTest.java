package com.cds.utils.es.cache;

/**
 * Created by chendongsheng5 on 2017/5/22.
 */
public class CacheTest implements RemovalListener<String, String>{

  @Override
  public void onRemoval(RemovalNotification<String, String> notification) {
    System.out.println("ok");
  }

  public static void main(String[] args) {
    CacheTest test = new CacheTest();
    CacheBuilder builder = CacheBuilder.<String, String>builder().removalListener(test);
    Cache cache = builder.build();
    cache.put("a", "b");
    System.out.println(cache.get("a"));
    cache.invalidateAll();
    System.out.println();
  }
}
