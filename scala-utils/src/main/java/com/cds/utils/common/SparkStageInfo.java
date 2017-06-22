package com.cds.utils.common;

import java.io.Serializable;

/**
 * Created by chendongsheng5 on 2017/6/22.
 */
public interface SparkStageInfo extends Serializable {
  int stageId();
  int currentAttemptId();
  long submissionTime();
  String name();
  int numTasks();
  int numActiveTasks();
  int numCompletedTasks();
  int numFailedTasks();
}
