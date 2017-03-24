package com.cds.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * <p></p>
 * author chendongsheng5 2017/2/18 10:25
 * version V1.0
 * modificationHistory =========================逻辑或功能性重大变更记录
 * modify by user: chendongsheng5 2017/2/18 10:25
 * modify by reason:{方法名}:{原因}
 */
public class BatchQueue<T> {

  private final int batchSize;
  private final Consumer<List<T>> consumer;
  private AtomicBoolean inUse = new AtomicBoolean(true);
  private BlockingQueue<T> queue = new LinkedBlockingQueue<>();

  public BatchQueue(int batchSize, Consumer<List<T>> consumer) {
    this.batchSize = batchSize;
    this.consumer = consumer;

    startLoop();
  }

  public static void main(String[] args) {
    BatchQueue<String> batchQueue = new BatchQueue<>(3, System.out::println);
    while (true) {
      String line = new Scanner(System.in).nextLine();
      if (line.equals("done")) {
        batchQueue.shutdown();
        break;
      }
      batchQueue.add(line);
    }
  }

  public boolean add(T t) {
    if (!inUse.get()) {
      throw new RuntimeException("This queue is aready shutdown");
    }
    return queue.add(t);
  }

  public void shutdown() {
    inUse.set(false);
  }

  private void startLoop() {
    new Thread(() -> {
      while (inUse.get()) {
        if (queue.size() >= batchSize) {
          drainToConsume();
        }
      }
      drainToConsume();
    }).start();
  }

  private void drainToConsume() {
    List<T> drained = new ArrayList<>();
    queue.drainTo(drained, batchSize);
    consumer.accept(drained);
  }
}
