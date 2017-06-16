/**
 * Created by chendongsheng5 on 2017/6/16.
 * Example
 * </p>
 * <pre>
 * {
 *   &#064;code
 *   RetryPolicy retry = new ExponentialBackoffRetry(50, Constants.SECOND_MS, MAX_CONNECT_TRY);
 *   do {
 *     // work to retry
 *   } while (retry.attemptRetry());
 * }
 * </pre>
 */
package com.cds.common.retry;