package com.wyy.tool.task;

import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.wyy.tool.common.ToolOperator.*;

public class CreateTask extends AbstractTask {
    final static Logger log = LoggerFactory.getLogger(CreateTask.class);

    public CreateTask(Configuration conf) {
        super(conf);
        qpsMeter = MetricsSystem.meter(this.getClass(), "request", "qps");
        iopsMeter = MetricsSystem.meter(this.getClass(), "request", "iops");
        timer = MetricsSystem.timer(this.getClass(), "request", "latency");
    }

    public void doTask() {
        int totalFiles = ToolConfig.getInstance().getTotalFiles();
        int totalThreads = ToolConfig.getInstance().getTotalThreads();
        String nameType = ToolConfig.getInstance().getCreateFileNameType();
        String userName = ToolConfig.getInstance().getUserName();
        String HostName = ToolConfig.getInstance().getHost();
        String workPath = ToolConfig.getInstance().getWorkPath();
        String filePrefix = ToolConfig.getInstance().getCreateFilePrefix();

        //prepare test file
        long fileSize = ToolConfig.getInstance().getCreateSizePerFile();
        String src = ToolConfig.getInstance().getCreateFilePath();
        if (fileSize != 0) {
            prepareTestFile(fileSize, src);
        }

        int SingleFileNum = totalFiles / totalThreads;
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch Latch = new CountDownLatch(totalThreads);
        Map<Integer, List<String>> nameLists = new HashMap();
        //prepare name list
        if ("random".equals(nameType)) {
            for (int i = 0; i < totalThreads; i++) {
                List<String> names = new ArrayList<>();
                for (int j = 0; j < SingleFileNum; j++) {
                    names.add(RandomStringUtils.randomAlphanumeric(10));
                }
                nameLists.put(i, names);
            }
        } else {
            for (int i = 0; i < totalThreads; i++) {
                int startNum = i * SingleFileNum;
                int endNum = (i + 1) * SingleFileNum;
                List<String> names = new ArrayList<>();
                for (int j = startNum; j < endNum; j++) {
                    names.add(filePrefix + i);
                }
                nameLists.put(i, names);
            }
        }

        startTime = System.currentTimeMillis();
        MetricsSystem.startReport();
        // submit task
        for (int i = 0; i < totalThreads; i++) {
            String dst = HostName + workPath + "/TestThread-" + i + "/";
            SubTask hlt = new SubTask(src, dst, userName, nameLists.get(i), fileSize, conf, Latch);
            ThreadPool.execute(hlt);
        }

        //wait result
        try {
            Latch.await();
        } catch (InterruptedException e) {
            log.warn("CreateTask: execute task error,exception:", e);
        } finally {
            ThreadPool.shutdown();
            MetricsSystem.stopReport();
        }
    }

    private void prepareTestFile(long fileSize, String src) {
        try {
            File file = new File(src);
            FileOutputStream fileOut = new FileOutputStream(file, false);
            int bufferSize = 1024;
            while (fileSize > bufferSize) {
                String randomStr = RandomStringUtils.randomAlphanumeric(bufferSize);
                fileOut.write(randomStr.getBytes());
                fileSize -= bufferSize;
            }
            String randomStr = RandomStringUtils.randomAlphanumeric((int) fileSize);
            fileOut.write(randomStr.getBytes());
            fileOut.close();
        } catch (Exception e) {
            log.warn("when prepare test file meet exception:", e);
            throw new RuntimeException(e);
        }
    }


    class SubTask implements Runnable {
        private String src;
        private String dst;
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch Latch;
        private long fileSize;

        public SubTask(String src, String dst, String user, List<String> nameList, long fileSize, Configuration conf,
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
                FileSystem fs = FileSystem.get(uri, conf, user);
                String putFilePrefix = ToolConfig.getInstance().getCreateFilePrefix();
                for (String name : nameList) {
                    String tmpdst = dst + putFilePrefix + name;
                    boolean ret;
                    try(Timer.Context context = timer.time()) {
                        ret = putToFS(src, tmpdst, fs);
                    }
                    if (!ret) {
                        log.warn("write : put file to hdfs failed, file:" + tmpdst);
                    } else {
                        iopsMeter.mark(fileSize);
                    }
                    qpsMeter.mark();
                }
            } catch (Exception e) {
                log.error("write task exception:", e);
            } finally {
                Latch.countDown();
            }
        }
    }
}