package com.wyy.tool.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.ToolConfig;
import com.wyy.tool.tool.ToolHttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetReadTask extends AbstractTask{
  final static Logger log = LoggerFactory.getLogger(ListTask.class);
  AtomicBoolean end = new AtomicBoolean(false);
  String operation;

  public ParquetReadTask(Configuration conf, String operation) {
    super(conf);
    this.operation = operation;
    qpsMeter = MetricsSystem.meter(this.getClass(), "request", "qps");
    timer = MetricsSystem.timer(this.getClass(), "request", "latency");
  }

  @Override
  public void doTask() {
    long durationTime = ToolConfig.getInstance().getReadDurationTime();
    startTime = System.currentTimeMillis();
    MetricsSystem.startReport();
    String dst = ToolConfig.getInstance().getParquetRequestPath();
    for (int i = 0; i < totalThreads; i++) {
      Runnable task = new ParquetReadSubTask(dst, latch);
      threadPool.execute(task);
    }

    try {
      latch.await(durationTime, TimeUnit.SECONDS);
      end.set(true);
      log.warn("all tasks should meet the end.");
      latch.await(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.warn("CheckStatusTask: execute task error,exception:", e);
    } finally {
      threadPool.shutdownNow();
      MetricsSystem.stopReport();
    }
  }

  class ParquetReadSubTask implements Runnable {
    private String targetPath;
    private CountDownLatch latch;

    public ParquetReadSubTask(String targetPath, CountDownLatch latch) {
      this.targetPath = targetPath;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        String restHost = ToolConfig.getInstance().getRestHost();
        String restUrl = restHost + targetPath;
        while (true) {
          if (end.get()) {
            return;
          }

          try (Timer.Context context = timer.time()) {
            Header header = new BasicHeader("transfer-type", "chunked");
            ToolHttpClient.getInstance().httpGetStream(restUrl, iopsMeter, header);
          } finally {
            qpsMeter.mark();
          }
        }
      } catch (Throwable e) {
        log.error("ParquetReadSubTask: execute task error,exception:", e);
      } finally {
        latch.countDown();
      }
    }
  }
}
