package com.wyy.tool.task;

import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReadFileTask extends AbstractTask {
    final static Logger log = LoggerFactory.getLogger(ReadFileTask.class);

    public ReadFileTask(Configuration conf) {
        super(conf);
        qpsMeter = MetricsSystem.meter(this.getClass(), "request", "qps");
        iopsMeter = MetricsSystem.meter(this.getClass(), "request", "iops");
        timer = MetricsSystem.timer(this.getClass(), "request", "latency");
    }

    public void doTask() {
        int totalThreads = ToolConfig.getInstance().getTotalThreads();
        String userName = ToolConfig.getInstance().getUserName();
        String HostName = ToolConfig.getInstance().getHost();
        String workPath = ToolConfig.getInstance().getWorkPath();
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch Latch = new CountDownLatch(totalThreads);
        long durationTime = ToolConfig.getInstance().getReadDurationTime();

        Map<Integer, List<String>> nameLists = new HashMap();
        try {
            URI uri = new URI(HostName + workPath);
            FileSystem fs = FileSystem.get(uri, conf, userName);
            for (int i = 0;i < totalThreads;i++) {
                String dst = HostName + workPath + "/TestThread-" + i + "/";
                FileStatus[] fileStatuses = fs.listStatus(new Path(dst));
                List<String> names = new ArrayList<>();
                for (FileStatus status : fileStatuses) {
                    names.add(dst + status.getPath().getName());
                }
                nameLists.put(i, names);
            }
        } catch (Exception e) {
            log.warn("list file exception:", e);
            throw new RuntimeException(e);
        }

        MetricsSystem.startReport();
        List<Future> futureList = new ArrayList<>();
        for (int i = 0; i < totalThreads; i++) {
            SubTask hlt = new SubTask(userName, nameLists.get(i), conf, Latch);
            Future future = ThreadPool.submit(hlt);
            futureList.add(future);
        }

        try {
//            Latch.await();
            futureList.get(0).get(durationTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("CheckStatusTask: execute task error,exception:", e);
        } finally {
            for (Future future : futureList) {
                future.cancel(true);
            }
            ThreadPool.shutdown();
            MetricsSystem.stopReport();
        }
    }

    class SubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch latch;

        public SubTask(String user, List<String> nameList, Configuration conf,
                       CountDownLatch latch) {
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
            while (true) {
                FileSystem fs;
                try {
                    URI uri = new URI(nameList.get(0));
                    fs = FileSystem.get(uri, conf, user);
                } catch (Exception e) {
                    log.error("read task exception:", e);
                    return;
                }
                for (String path : nameList) {
                    Path srcPath = new Path(path);
                    byte[] b = new byte[1048576];
                    int total = 0;
                    int length;
                    FSDataInputStream in = null;
                    try (Timer.Context context = timer.time()) {
                        in = fs.open(srcPath);
                        while ((length = in.read(b)) > 0) {
                            total = total + length;
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
        }
    }
}
