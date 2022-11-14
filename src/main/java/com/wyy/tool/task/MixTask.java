package com.wyy.tool.task;

import static com.wyy.tool.common.ToolOperator.putToFS;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.ToolConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MixTask extends AbstractTask {
  final static Logger log = LoggerFactory.getLogger(ReadFileTask.class);
  AtomicBoolean end = new AtomicBoolean(false);

  public MixTask(Configuration conf) {
    super(conf);
    qpsMeter = MetricsSystem.meter(this.getClass(), "mix", "qps");
    iopsMeter = MetricsSystem.meter(this.getClass(), "mix", "iops");
    timer = MetricsSystem.timer(this.getClass(), "mix", "latency");
  }

  @Override
  public void doTask() {
    int totalFiles = ToolConfig.getInstance().getTotalFiles();
    String HostName = ToolConfig.getInstance().getHost();
    String workPath = ToolConfig.getInstance().getWorkPath();
    String userName = ToolConfig.getInstance().getUserName();
    int readPercentage = ToolConfig.getInstance().getMixReadPercentage();
    int createPercentage = ToolConfig.getInstance().getMixCreatePercentage();
    int totalThreadCount = ToolConfig.getInstance().getTotalThreads();
    int readThreadCount = totalThreadCount * readPercentage / 100;
    int createThreadCount = totalThreadCount * createPercentage / 100;
    long durationTime = ToolConfig.getInstance().getReadDurationTime();
    String nameType = ToolConfig.getInstance().getCreateFileNameType();
    String filePrefix = ToolConfig.getInstance().getCreateFilePrefix();
    if (readThreadCount + createThreadCount > totalThreadCount) {
      log.warn("invalid parameter! create percentage is {}, read percentage is {}.", createPercentage, readPercentage);
      //TODO close resource
      return;
    }

    //prepare test file
    long fileSize = ToolConfig.getInstance().getCreateSizePerFile();
    String src = ToolConfig.getInstance().getCreateFilePath();
    if (fileSize >= 0) {
      CreateTask.prepareTestFile(fileSize, src);
    }

    //prepare read name list
    int readFileNumber = totalFiles * readPercentage / 100;
    int readFileNumPerThread = readFileNumber / readThreadCount;
    Map<Integer, List<String>> readNameLists =
        CreateTask.prepareNamelists(readThreadCount, nameType, filePrefix, readFileNumPerThread);

    //prepare create name list
    int createFileNumber = totalFiles * createPercentage / 100;
    int createFileNumPerThread = createFileNumber / createThreadCount;
    Map<Integer, List<String>> createNameLists =
        CreateTask.prepareNamelists(createThreadCount, nameType, filePrefix, createFileNumPerThread);

    //upload some test files before read
    if (!prepareReadFiles(readThreadCount, fileSize, src, readNameLists)) {
      return;
    }

    // start real task
    startTime = System.currentTimeMillis();
    MetricsSystem.startReport();
    for (int i = 0; i < createThreadCount; i++) {
      String dst = HostName + workPath + "/create-TestThread-" + i + "/";
      CreateSubTask hlt =
          new CreateSubTask(src, dst, userName, createNameLists.get(i), fileSize, conf, latch);
      threadPool.execute(hlt);
    }
    for (int i = 0; i < readThreadCount; i++) {
      String dst = HostName + workPath + "/read-TestThread-" + i + "/";
      ReadSubTask hlt =
          new ReadSubTask(dst, userName, readNameLists.get(i), conf, latch);
      threadPool.execute(hlt);
    }

    try {
      latch.await(durationTime, TimeUnit.SECONDS);
      end.set(true);
      log.warn("all tasks should meet the end.");
      latch.await();
    } catch (Exception e) {
      log.warn("CheckStatusTask: execute task error,exception:", e);
    } finally {
      threadPool.shutdownNow();
      MetricsSystem.stopReport();
    }
  }

  private boolean prepareReadFiles(int readThreadCount, long fileSize, String src, Map<Integer, List<String>> readNameLists) {
    String HostName = ToolConfig.getInstance().getHost();
    String workPath = ToolConfig.getInstance().getWorkPath();
    String userName = ToolConfig.getInstance().getUserName();
    CountDownLatch countDownLatch = new CountDownLatch(readThreadCount);
    for (int i = 0; i < readThreadCount; i++) {
      String dst = HostName + workPath + "/read-TestThread-" + i + "/";
      CreateTask task = new CreateTask(conf);
      CreateTask.SubTask hlt =
          task.new SubTask(src, dst, userName, readNameLists.get(i), fileSize, conf,
              countDownLatch);
      threadPool.execute(hlt);
    }
    try {
      countDownLatch.await();
    } catch (Exception e) {
      log.warn("mix task fail: prepare files to read fail, exception:", e);
      return false;
    }
    return true;
  }

  class CreateSubTask implements Runnable {

    private String src;
    private String dst;
    private String user;
    private List<String> nameList;
    private Configuration conf;
    private CountDownLatch Latch;
    private long fileSize;

    public CreateSubTask(String src, String dst, String user, List<String> nameList, long fileSize, Configuration conf,
                   CountDownLatch latch) {
      this.src = src;
      this.dst = dst;
      this.user = user;
      this.nameList = nameList;
      this.fileSize = fileSize;
      this.conf = conf;
      Latch = latch;
    }

    @Override
    public void run() {
      try {
        URI uri = new URI(dst);
        String filePrefix = ToolConfig.getInstance().getCreateFilePrefix();
        FileSystem fs = FileSystem.get(uri, conf, user);
        while (true) {
          if (end.get()) {
            return;
          }
          boolean ret;
          String tmpdst = dst + filePrefix + RandomStringUtils.randomAlphanumeric(10);
          try (Timer.Context context = timer.time()) {
            ret = putToFS(src, tmpdst, fs);
          }
          if (!ret) {
            log.warn("write : put file to hdfs failed, file:" + tmpdst);
          } else {
            qpsMeter.mark();
            iopsMeter.mark(fileSize);
          }
        }
      } catch (Exception e) {
        log.error("write task exception:", e);
      } finally {
        Latch.countDown();
      }
    }
  }

  class ReadSubTask implements Runnable {
    private String dst;
    private String user;
    private List<String> nameList;
    private Configuration conf;
    private CountDownLatch latch;

    public ReadSubTask(String dst, String user, List<String> nameList, Configuration conf,
                       CountDownLatch latch) {
      this.dst = dst;
      this.user = user;
      this.nameList = nameList;
      this.conf = conf;
      this.latch = latch;
    }

    @Override
    public void run() {
      if (nameList.isEmpty()) {
        return;
      }
      try {
        while (true) {
          FileSystem fs;
          URI uri = new URI(dst);
          fs = FileSystem.get(uri, conf, user);
          for (String path : nameList) {
            if (end.get()) {
              return;
            }
            String tmpdst = dst + path;
            Path srcPath = new Path(tmpdst);
            int bufferSize = ToolConfig.getInstance().getReadBufferSize();
            byte[] b = new byte[bufferSize];
            int length;
            FSDataInputStream in = null;
            try (Timer.Context context = timer.time()) {
              in = fs.open(srcPath);
              BufferedInputStream buffer = new BufferedInputStream(in, bufferSize);
              while ((length = buffer.read(b)) > 0) {
                iopsMeter.mark(length);
              }
            } catch (IOException e) {
              log.error("read file " + path + " failed! ", e);
            } finally {
              if (in != null) {
                try {
                  in.close();
                } catch (IOException ioException) {
                  log.warn("read file " + path + " close stream failed. ");
                }
              }
              qpsMeter.mark();
            }
          }
        }
      } catch (Exception e) {
        log.error("read task exception:", e);
        return;
      } finally {
        latch.countDown();
      }
    }
  }
}