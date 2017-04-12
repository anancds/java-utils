package com.cds.utils.uuid;

import java.security.SecureRandom;

/**
 * Created by chendongsheng5 on 2017/4/12.
 */
class SecureRandomHolder {
  // class loading is atomic - this is a lazy & safe singleton to be used by this package
  public static final SecureRandom INSTANCE = new SecureRandom();
}
