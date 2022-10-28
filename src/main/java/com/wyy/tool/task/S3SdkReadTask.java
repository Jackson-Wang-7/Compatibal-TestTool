package com.wyy.tool.task;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.ToolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3SdkReadTask extends AbstractTask{
  final static Logger log = LoggerFactory.getLogger(S3SdkReadTask.class);

  public S3SdkReadTask(Configuration conf) {
    super(conf);
    qpsMeter = MetricsSystem.meter(this.getClass(), "request", "qps");
    iopsMeter = MetricsSystem.meter(this.getClass(), "request", "iops");
    timer = MetricsSystem.timer(this.getClass(), "request", "latency");
  }

  @Override
  public void doTask() {
    int totalThreads = ToolConfig.getInstance().getTotalThreads();
    String userName = ToolConfig.getInstance().getUserName();
    String HostName = ToolConfig.getInstance().getHost();
    String workPath = ToolConfig.getInstance().getWorkPath();
    ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
    CountDownLatch Latch = new CountDownLatch(totalThreads);


    Map<Integer, List<String>> nameLists = new HashMap();
    try {
      URI uri = new URI(HostName + workPath);
      FileSystem fs = FileSystem.get(uri, conf, userName);
      for (int i = 0;i < totalThreads;i++) {
        String dst = HostName + workPath + "/TestThread-" + i + "/";
        FileStatus[] fileStatuses = fs.listStatus(new Path(dst));
        List<String> names = new ArrayList<>();
        for (FileStatus status : fileStatuses) {
          names.add(workPath + "/TestThread-" + i + "/" + status.getPath().getName());
        }
        nameLists.put(i, names);
      }
    } catch (Exception e) {
      log.warn("list file exception:", e);
      throw new RuntimeException(e);
    }
  }

  class SubTask implements Runnable {

    @Override
    public void run() {
//      AmazonS3 s3client = AmazonS3Client.builder().withEndpointConfiguration()
    }
  }
}
