package com.wyy.tool.task;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTask {
  final static Logger log = LoggerFactory.getLogger(AbstractTask.class);
  Meter qpsMeter;
  Meter iopsMeter;

  Timer timer;
  Configuration conf;

  TimeUnit durationUnit = TimeUnit.MILLISECONDS;
  TimeUnit rateUnit = TimeUnit.SECONDS;

  public AbstractTask(Configuration conf) {
    this.conf = conf;
  }

  public void start() {
    long startTime = System.currentTimeMillis();
    try {
      doTask();
    } finally {
      reportMetrics(System.currentTimeMillis() - startTime);
    }
  }

  public abstract void doTask();

  public void reportMetrics(long totalTime) {
    if (qpsMeter != null) {
      log.warn("put qps mean: {} req/s", qpsMeter.getMeanRate());
    }
    if (iopsMeter != null) {
      log.warn("put IOps mean:" + iopsMeter.getMeanRate());
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
