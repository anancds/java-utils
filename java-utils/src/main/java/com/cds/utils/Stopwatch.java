package com.cds.utils;

import java.util.logging.Logger;

/**
 * Created by chendongsheng5 on 2017/4/14.
 */
public class Stopwatch {

  private static final Logger logger = Logger.getLogger(Stopwatch.class.getName());

  private long startNS = System.nanoTime();

  /**
   * Resets and returns elapsed time in milliseconds.
   */
  public long reset() {
    long nowNS = System.nanoTime();
    try {
      return TimeValue.nsecToMSec(nowNS - startNS);
    } finally {
      startNS = nowNS;
    }
  }

  /**
   * Resets and logs elapsed time in milliseconds.
   */
  public void resetAndLog(String label) {
    logger.fine(label + ": " + reset() + "ms");
  }
}
