package com.wyy.tool.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.ToolConfig;
import com.wyy.tool.tool.Util;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListTask extends AbstractTask {
  final static Logger log = LoggerFactory.getLogger(ListTask.class);
  AtomicBoolean end = new AtomicBoolean(false);
  String operation;

  public ListTask(Configuration conf, String operation) {
    super(conf);
    this.operation = operation;
    qpsMeter = MetricsSystem.meter(this.getClass(), "request", "qps");
    timer = MetricsSystem.timer(this.getClass(), "request", "latency");
  }

  @Override
  public void doTask() {
    long durationTime = ToolConfig.getInstance().getReadDurationTime();
    String workPath = ToolConfig.getInstance().getWorkPath();
    workPath = Util.trimToObjectPath(workPath);

    boolean listThread = ToolConfig.getInstance().isListThreadPrefix();
    startTime = System.currentTimeMillis();
    MetricsSystem.startReport();
    for (int i = 0; i < totalThreads; i++) {
      String dst;
      if (listThread) {
        int currentIndex = i % 20;
        dst = workPath + "/TestThread-" + currentIndex + "/";
      } else {
        dst = workPath + "/";
      }
      Runnable task = new S3ListTask(dst, latch);
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

  class S3ListTask implements Runnable {
    private String targetPath;
    private CountDownLatch latch;

    public S3ListTask(String targetPath, CountDownLatch latch) {
      this.targetPath = targetPath;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        String restHost = ToolConfig.getInstance().getRestHost();
        String bucket = ToolConfig.getInstance().getBucketName();
        bucket = Util.trimToObjectPath(bucket);
        String user = ToolConfig.getInstance().getUserName();
        boolean keepAlive = ToolConfig.getInstance().isTcpKeepAlive();
        ClientConfiguration clientConfiguration =
            new ClientConfiguration().withSocketTimeout(300 * 1000)
                .withTcpKeepAlive(keepAlive);

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(user, "secretKey")))
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(restHost, ""))
            .withClientConfiguration(clientConfiguration)
            .enablePathStyleAccess()
            .build();

        String marker = "";
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(targetPath);
        while (true) {
          if (end.get()) {
            return;
          }
          request.setMarker(marker);
          try (Timer.Context context = timer.time()) {
            ObjectListing objectListing = s3.listObjects(request);
            if (objectListing.isTruncated()) {
              marker = objectListing.getNextMarker();
            } else {
              marker = "";
            }
          } finally {
            qpsMeter.mark();
          }
        }
      } finally {
        latch.countDown();
      }
    }
  }
}
