package com.cds.utils.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomNameThreadFactory implements ThreadFactory {

  static final AtomicInteger poolNumber = new AtomicInteger(1);
  private String unitName;

  /**
   * 构造函数
   *
   * @param unitName - 自定义线程名
   */
  public CustomNameThreadFactory(String unitName) {
    this.unitName = unitName;
  }

  public static void main(String[] args) {
    System.out.println(CustomNameThreadFactory.class.getSimpleName());

    ThreadFactory factory = new CustomNameThreadFactory("abc");

    Thread thread = factory.newThread(new Runnable() {
      @Override
      public void run() {
        System.out.println("hello");
      }
    });

    System.out.println(thread.getName());
  }

  @Override
  public Thread newThread(Runnable r) {

    return new Thread(r, unitName + "-" + poolNumber.getAndIncrement());
  }
}
