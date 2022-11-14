package com.wyy.tool.task;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.wyy.tool.common.ToolConfig;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTask {
  final static Logger log = LoggerFactory.getLogger(AbstractTask.class);
  Meter qpsMeter;
  Meter iopsMeter;
  Timer timer;
  Configuration conf;
  int totalThreads;
  ExecutorService threadPool;
  CountDownLatch latch;

  TimeUnit durationUnit = TimeUnit.MILLISECONDS;
  TimeUnit rateUnit = TimeUnit.SECONDS;
  long startTime;

  public AbstractTask(Configuration conf) {
    this.conf = conf;
    totalThreads = ToolConfig.getInstance().getTotalThreads();
    threadPool = Executors.newFixedThreadPool(totalThreads);
    latch = new CountDownLatch(totalThreads);
  }

  public void start() {
    startTime = System.currentTimeMillis();
    try {
      doTask();
    } finally {
      reportMetrics(System.currentTimeMillis() - startTime);
    }
  }

  public abstract void doTask();

  public void reportMetrics(long totalTime) {
    if (qpsMeter != null) {
      log.warn("op qps mean: {} req/s", qpsMeter.getMeanRate());
    }
    if (iopsMeter != null) {
      log.warn("op IOps mean: {}/s", this.convertSize(Double.valueOf(iopsMeter.getMeanRate()).longValue()));
    }
    if (timer != null) {
      Snapshot snapshot = timer.getSnapshot();
      log.warn("count = {}", timer.getCount());
      log.warn(String.format("mean rate = %2.2f calls/%s", this.convertRate(timer.getMeanRate()), this.getRateUnit()));
      log.warn(String.format("min = %2.2f %s", this.convertDuration((double)snapshot.getMin()), this.getDurationUnit()));
      log.warn(String.format("max = %2.2f %s", this.convertDuration((double)snapshot.getMax()), this.getDurationUnit()));
      log.warn(String.format("mean = %2.2f %s", this.convertDuration(snapshot.getMean()), this.getDurationUnit()));
      log.warn(String.format("median = %2.2f %s", this.convertDuration(snapshot.getMedian()), this.getDurationUnit()));
      log.warn(String.format("75%% <= %2.2f %s", this.convertDuration(snapshot.get75thPercentile()), this.getDurationUnit()));
      log.warn(String.format("95%% <= %2.2f %s", this.convertDuration(snapshot.get95thPercentile()), this.getDurationUnit()));
      log.warn(String.format("99%% <= %2.2f %s", this.convertDuration(snapshot.get99thPercentile()), this.getDurationUnit()));
    }
    log.warn("total time: {}ms", totalTime);
  }


  /**
   * long to kb/mb/gb
   * @param size long
   * @return
   */
  public  String convertSize(long size) {
    if (size < 1024) {
      return String.valueOf(size) + "B";
    } else {
      size = size / 1024;
    }
    if (size < 1024) {
      return String.valueOf(size) + "KB";
    } else {
      size = size / 1024;
    }
    if (size < 1024) {
      size = size * 100;
      return String.valueOf((size / 100)) + "."
          + String.valueOf((size % 100)) + "MB";
    } else {
      size = size * 100 / 1024;
      return String.valueOf((size / 100)) + "."
          + String.valueOf((size % 100)) + "GB";
    }
  }

  protected double convertDuration(double duration) {
    long durationFactor = durationUnit.toNanos(1L);
    return duration / (double)durationFactor;
  }

  protected String getDurationUnit() {
    return durationUnit.toString().toLowerCase(Locale.US);
  }

  protected double convertRate(double rate) {
    long rateFactor = rateUnit.toSeconds(1L);
    return rate * (double)rateFactor;
  }

  protected String getRateUnit() {
    String s = rateUnit.toString().toLowerCase(Locale.US);
    return s.substring(0, s.length() - 1);
  }
}
