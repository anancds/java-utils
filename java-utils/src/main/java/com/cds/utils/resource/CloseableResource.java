package com.cds.utils.resource;

import java.io.Closeable;

/**
 * A {@code CloseableResource<T>} is a wrapper around a resource of type {@code T} which must do
 * some sort of cleanup when it is no longer in use.
 *
 * @param <T> the type of the wrapped resource
 */
public abstract class CloseableResource<T> implements Closeable {
  private T mResource;

  /**
   * Creates a {@link CloseableResource} wrapper around the given resource. This resource will
   * be returned by the {@link CloseableResource#get()} method.
   *
   * @param resource the resource to wrap
   */
  public CloseableResource(T resource) {
    mResource = resource;
  }

  /**
   * @return the resource
   */
  public T get() {
    return mResource;
  }

  /**
   * Performs any cleanup operations necessary when the resource is no longer in use.
   */
  @Override
  public abstract void close();
}
