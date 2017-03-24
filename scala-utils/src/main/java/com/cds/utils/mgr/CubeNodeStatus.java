package com.cds.utils.mgr;

import java.io.IOException;

/**
 * Created by chendongsheng5 on 2017/3/24.
 */
public enum CubeNodeStatus {

  USABLE(1),      // 可用的
  UNUSABLE(0),    // 不可用的
  DEPRECATED(-1), // 弃用的
  INVALID(-2);    // 非法状态值

  private int value;

  private CubeNodeStatus(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }

  public static CubeNodeStatus valueOf(int value) throws IOException {

    switch (value) {

      case 1:
        return USABLE;
      case 0:
        return UNUSABLE;
      case -1:
        return DEPRECATED;
      case -2:
        return INVALID;
      default:
        throw new IOException("unsupported cube node status.");
    }
  }
}
