package com.cds.utils.lease;

import java.io.Closeable;

/**
 * Created by chendongsheng5 on 2017/4/12.
 * 自动关闭资源的接口
 */
public interface Releasable extends Closeable {

  @Override
  void close();

}
