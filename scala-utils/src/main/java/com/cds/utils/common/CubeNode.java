package com.cds.utils.common;

import com.cds.utils.mgr.CubeNodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chendongsheng5 on 2017/3/24.
 */
public class CubeNode {

  private static final Logger LOG = LoggerFactory.getLogger(CubeNode.class);

  private String nodeName;
  private String start;
  private String end;
  private String tier; // 所属层级
  private CubeNodeStatus status = CubeNodeStatus.UNUSABLE;
  private Boolean isProcessing = false; // 是否处于正在处理状态
  private final byte[] processLock = new byte[0]; // 处理状态锁

  // 是否可信，不可信表示可能存在数据不一致问题，需要校验重构
  // 0为可信，其它任何值均为不可信
  private int reliability = 0;

  public CubeNode(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getNodeName() {
    return nodeName;
  }

  public String getStart() {
    return start;
  }

  public void setStart(String start) {
    this.start = start;
  }

  public String getEnd() {
    return end;
  }

  public void setEnd(String end) {
    this.end = end;
  }

  public String getTier() {
    return tier;
  }

  public void setTier(String tier) {
    this.tier = tier;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public synchronized CubeNodeStatus getStatus() {
    return status;
  }

  public int getReliability() {
    return reliability;
  }

  public void setReliability(int reliability) {
    this.reliability = reliability;
  }

  /**
   * 只能设为USABLE、UNUSABLE、DEPRECATED
   */
  public synchronized void setStatus(CubeNodeStatus status) {
    this.status = status;
  }

  /**
   * 对节点操作处理结束后调用
   * 设置为处理完成状态，状态设置完成后，通知正在获取的线程
   */
  public void setProcessed() {

    synchronized (processLock) {
      LOG.trace("{}, {}: will to set cubeNode({}) is not in processing, isProcessing={}.", Thread.currentThread().getId
          (), System.currentTimeMillis(), this.nodeName, this.isProcessing);
      if (this.isProcessing) {
        this.isProcessing = false;
        // 唤醒其它等待处理的线程
        this.processLock.notify();
        LOG.trace("{}, {}: begin to set cubeNode({}) is not in processing, isProcessing={}.", Thread.currentThread().getId
            (), System.currentTimeMillis(), this.nodeName, this.isProcessing);
      }
    }
  }

  /**
   * 处理成功恢复一点可信度，方法必须在setProcessing与setProcessed之间使用
   */
  public void setProcessSucceeded() {

    // 成功处理后, 首先恢复可信度，可信度增1
    increaseReliability();
  }

  /**
   * 对节点操作处理前调用
   * 设置为正在处理状态，此时若已经在处理状态，则阻塞等待，直到上一个处理流程做完
   *
   * @throws InterruptedException
   */
  public void setProcessing() throws InterruptedException {

    synchronized (processLock) {
      LOG.trace("{}, {}: will to set cubeNode({}) is in processing, isProcessing={}.", Thread.currentThread().getId
          (), System.currentTimeMillis(), this.nodeName, this.isProcessing);
      // 获取处理资格后，首先将可信度减1
      decreaseReliability();
      if (this.isProcessing) {
        // 如果有其它流程正在处理本节点，则等待
        processLock.wait();
      }
      LOG.trace("{}, {}: begin to set cubeNode({}) is in processing, isProcessing={}.", Thread.currentThread().getId
          (), System.currentTimeMillis(), this.nodeName, this.isProcessing);
      this.isProcessing = true;
    }
  }

  /**
   * 该cube节点是否可信，不可信表示可能存在数据不一致问题，需要校验重构
   * 调用前需要先调用setProcessing()方法获取处理资格
   *
   * @return false表示不可信，反之可信
   */
  public boolean isReliable() {

    return (0 == reliability || (-1 == reliability && isProcessing));
  }

  private void increaseReliability() {

    if (0 <= reliability)
      return;
    this.reliability++;
  }

  private void decreaseReliability() {

    this.reliability--;
  }

  @Override
  public String toString(){
    return "{\"nodeName\":\"" + nodeName + "\"," +
        "\"start\":\"" + start + "\"," +
        "\"end\":\"" + end + "\"," +
        "\"tier\":\"" + tier + "\"," +
        "\"status\":\"" + status.value() + "\"," +
        "\"reliability\":\"" + reliability + "\"}";
  }
}
