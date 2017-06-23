package com.cds.utils.common.progressbar

import com.cds.utils.common.SparkStageInfo

/**
  * Created by chendongsheng5 on 2017/6/22.
  */
private class SparkStageInfoImpl(
                                  val stageId: Int,
                                  val currentAttemptId: Int,
                                  val submissionTime: Long,
                                  val name: String,
                                  val numTasks: Int,
                                  val numActiveTasks: Int,
                                  val numCompletedTasks: Int,
                                  val numFailedTasks: Int)
  extends SparkStageInfo

