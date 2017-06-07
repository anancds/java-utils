package com.cds.utils.clock;

/**
 * An interface representing a clock. If we drop support for java 7 we could use java 8's
 * java.time.Clock instead.
 */
public interface Clock {
  /**
   * @return the current time in milliseconds
   */
  long millis();
}
