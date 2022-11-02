package com.wyy.tool.task;

import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.OpCode;
import com.wyy.tool.common.ToolOperator;
import com.wyy.tool.tool.ToolHttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReadFileTask extends AbstractTask {
    final static Logger log = LoggerFactory.getLogger(ReadFileTask.class);
    AtomicBoolean end = new AtomicBoolean(false);

    String operation;

    public ReadFileTask(Configuration conf, String operation) {
        super(conf);
        this.operation = operation;
        qpsMeter = MetricsSystem.meter(this.getClass(), operation, "qps");
        iopsMeter = MetricsSystem.meter(this.getClass(), operation, "iops");
        timer = MetricsSystem.timer(this.getClass(), operation, "latency");
    }

    public void doTask() {
        String userName = ToolConfig.getInstance().getUserName();
        String HostName = ToolConfig.getInstance().getHost();
        String workPath = ToolConfig.getInstance().getWorkPath();
        long durationTime = ToolConfig.getInstance().getReadDurationTime();

        Map<Integer, List<String>> nameLists = new HashMap();
        try {
            URI uri = new URI(HostName + workPath);
            FileSystem fs = FileSystem.get(uri, conf, userName);
            for (int i = 0; i < totalThreads; i++) {
                String dst = HostName + workPath + "/TestThread-" + i + "/";
                FileStatus[] fileStatuses = fs.listStatus(new Path(dst));
                List<String> names = new ArrayList<>();
                for (FileStatus status : fileStatuses) {
                    // rest read need the rest host name instead of default hostname
                    if (OpCode.REST_READ.getOpValue().equals(operation)) {
                        names.add(workPath + "/TestThread-" + i + "/" + status.getPath().getName());
                    } else {
                        names.add(dst + status.getPath().getName());
                    }
                }
                nameLists.put(i, names);
            }
        } catch (Exception e) {
            log.warn("list file exception:", e);
            throw new RuntimeException(e);
        }

        startTime = System.currentTimeMillis();
        MetricsSystem.startReport();
        for (int i = 0; i < totalThreads; i++) {
            Runnable task;
            if (OpCode.READ.getOpValue().equals(operation)) {
                task = new ReadSubTask(userName, nameLists.get(i), conf, Latch);
            } else if (OpCode.CHECK_STATUS.getOpValue().equals(operation)) {
                task = new CheckSubTask(userName, nameLists.get(i), conf, Latch);
            } else if (OpCode.REST_READ.getOpValue().equals(operation)) {
                task = new RestReadSubTask(userName, nameLists.get(i), Latch);
            } else {
                return;
            }
            ThreadPool.execute(task);
        }

        try {
            Latch.await(durationTime, TimeUnit.SECONDS);
            end.set(true);
            log.warn("all tasks should meet the end.");
            Latch.await();
        } catch (Exception e) {
            log.warn("CheckStatusTask: execute task error,exception:", e);
        } finally {
            ThreadPool.shutdownNow();
            MetricsSystem.stopReport();
        }
    }

    class ReadSubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch latch;

        public ReadSubTask(String user, List<String> nameList, Configuration conf,
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
            try {
                while (true) {
                    FileSystem fs;
                    URI uri = new URI(nameList.get(0));
                    fs = FileSystem.get(uri, conf, user);
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        Path srcPath = new Path(path);
                        char[] b = new char[1048576];
                        int length;
                        FSDataInputStream in = null;
                        try (Timer.Context context = timer.time()) {
                            in = fs.open(srcPath);
                            BufferedReader buffer = new BufferedReader(new InputStreamReader(in));
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

    class CheckSubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch latch;

        public CheckSubTask(String user, List<String> nameList, Configuration conf,
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
            try {
                while (true) {
                    FileSystem fs;
                    URI uri = new URI(nameList.get(0));
                    fs = FileSystem.get(uri, conf, user);
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        try (Timer.Context context = timer.time()) {
                            ToolOperator.checkFile(path, fs);
                        } finally {
                            qpsMeter.mark();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("read task exception:", e);
            } finally {
                latch.countDown();
            }
        }
    }

    class RestReadSubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private CountDownLatch latch;

        public RestReadSubTask(String user, List<String> nameList, CountDownLatch latch) {
            this.user = user;
            this.nameList = nameList;
            this.latch = latch;
        }

        @Override
        public void run() {
            if (nameList.isEmpty()) {
                return;
            }
            String restHost = ToolConfig.getInstance().getRestHost();
            try {
                while (true) {
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        try (Timer.Context context = timer.time()) {
                            String restUrl = restHost + path;
                            String Authorization = String.format("AWS4-HMAC-SHA256 Credential=%s/3uRmVm7lWfvclsqfpPJN2Ftigi4=", user);
                            Header header = new BasicHeader("Authorization", Authorization);
                            ToolHttpClient.httpGetStream(restUrl, iopsMeter, header);
                        } finally {
                            qpsMeter.mark();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("read task exception:", e);
            } finally {
                latch.countDown();
            }
        }
    }
}