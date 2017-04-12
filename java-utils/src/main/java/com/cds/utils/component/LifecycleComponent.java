package com.cds.utils.component;

import com.cds.utils.component.Lifecycle.State;
import com.cds.utils.lease.Releasable;

/**
 * Created by chendongsheng5 on 2017/4/12.
 */
public interface LifecycleComponent extends Releasable{
  State lifecycleState();

  void addLifecycleListener(LifecycleListener listener);

  void removeLifecycleListener(LifecycleListener listener);

  void start();

  void stop();
}
