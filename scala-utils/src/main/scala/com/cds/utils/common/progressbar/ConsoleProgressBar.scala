package com.cds.utils.common.progressbar

import java.util.{Timer, TimerTask}

import com.cds.utils.common.{Logging, SparkStageInfo}

/**
  * ConsoleProgressBar shows the progress of stages in the next line of the console. It poll the
  * status of active stages from `sc.statusTracker` periodically, the progress bar will be showed
  * up after the stage has ran at least 500ms. If multiple stages run in the same time, the status
  * of them will be combined together, showed in one line.
  */
private[common] class ConsoleProgressBar() extends Logging {
  // Carriage return
  private val CR = '\r'
  // Update period of progress bar, in milliseconds
  private val updatePeriodMSec = 200
  // Delay to show up a progress bar, in milliseconds
  private val firstDelayMSec = 500L

  // The width of terminal
  private val TerminalWidth = if (!sys.env.getOrElse("COLUMNS", "").isEmpty) {
    sys.env.get("COLUMNS").get.toInt
  } else {
    80
  }

  private var lastFinishTime = 0L
  private var lastUpdateTime = 0L
  private var lastProgressBar = ""

  // Schedule a refresh thread to run periodically
  private val timer = new Timer("refresh progress", false)
  timer.schedule(new TimerTask{
    override def run() {
      refresh()
    }
  }, firstDelayMSec, updatePeriodMSec)

  /**
    * Try to refresh the progress bar in every cycle
    */
  private def refresh(): Unit = synchronized {
    val now = System.currentTimeMillis()
    if (now - lastFinishTime < firstDelayMSec) {
      return
    }
    val stageIds = Array(1,2,3,4,5)
    val s1 = new SparkStageInfoImpl(1,1,1,"a",1,1,1,1)
    val s2 = new SparkStageInfoImpl(2,1,1,"b",1,1,1,1)
    val s3 = new SparkStageInfoImpl(3,1,1,"c",1,1,1,1)
    val s4 = new SparkStageInfoImpl(4,1,1,"d",1,1,1,1)
    val s5 = new SparkStageInfoImpl(5,1,1,"e",1,1,1,1)

    val stages = Array(s1, s2, s3, s4, s5)
    if (stages.length > 0) {
      show(now, stages.take(3))  // display at most 3 stages in same time
    }
  }

  /**
    * Show progress bar in console. The progress bar is displayed in the next line
    * after your last output, keeps overwriting itself to hold in one line. The logging will follow
    * the progress bar, then progress bar will be showed in next line without overwrite logs.
    */
  private def show(now: Long, stages: Seq[SparkStageInfo]) {
    val width = TerminalWidth / stages.size
    val bar = stages.map { s =>
      val total = s.numTasks()
      val header = s"[Stage ${s.stageId()}:"
      val tailer = s"(${s.numCompletedTasks()} + ${s.numActiveTasks()}) / $total]"
      val w = width - header.length - tailer.length
      val bar = if (w > 0) {
        val percent = w * s.numCompletedTasks() / total
        (0 until w).map { i =>
          if (i < percent) "=" else if (i == percent) ">" else " "
        }.mkString("")
      } else {
        ""
      }
      header + bar + tailer
    }.mkString("")

    // only refresh if it's changed OR after 1 minute (or the ssh connection will be closed
    // after idle some time)
    if (bar != lastProgressBar || now - lastUpdateTime > 60 * 1000L) {
      System.err.print(CR + bar)
      lastUpdateTime = now
    }
    lastProgressBar = bar
  }

  /**
    * Clear the progress bar if showed.
    */
  private def clear() {
    if (!lastProgressBar.isEmpty) {
      System.err.printf(CR + " " * TerminalWidth + CR)
      lastProgressBar = ""
    }
  }

  /**
    * Mark all the stages as finished, clear the progress bar if showed, then the progress will not
    * interweave with output of jobs.
    */
  def finishAll(): Unit = synchronized {
    clear()
    lastFinishTime = System.currentTimeMillis()
  }

  /**
    * Tear down the timer thread.  The timer thread is a GC root, and it retains the entire
    * SparkContext if it's not terminated.
    */
  def stop(): Unit = timer.cancel()


}
