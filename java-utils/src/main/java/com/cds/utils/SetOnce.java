package com.cds.utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by chendongsheng5 on 2017/4/8.
 */
public final class SetOnce<T> implements Cloneable {

  private final AtomicBoolean set;
  private volatile T obj = null;

  public SetOnce() {
    set = new AtomicBoolean(false);
  }

  public SetOnce(T obj) {
    this.obj = obj;
    set = new AtomicBoolean(true);
  }

  public static void main(String[] args) {
    SetOnce<String> once = new SetOnce<>();
    once.set("abc");
    System.out.println(once.get());
//    once.set("cde");
  }

  public final void set(T obj) {
    if (set.compareAndSet(false, true)) {
      this.obj = obj;
    } else {
      throw new AlreadySetException();
    }
  }

  public final T get() {
    return obj;
  }

  public static final class AlreadySetException extends IllegalStateException {

    public AlreadySetException() {
      super("The object cannot be set twice!");
    }
  }

}
