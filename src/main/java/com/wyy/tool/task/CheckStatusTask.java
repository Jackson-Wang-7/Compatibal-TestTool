package com.wyy.tool.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.wyy.tool.common.ToolOperator.*;

public class CheckStatusTask extends AbstractTask {

    final static Logger log = LoggerFactory.getLogger(CheckStatusTask.class);

    public CheckStatusTask(Configuration conf) {
        super(conf);
    }

    public void doTask() {
        int totalFiles = ToolConfig.getInstance().getTotalFiles();
        int totalThreads = ToolConfig.getInstance().getTotalThreads();
        int offset = ToolConfig.getInstance().getFileOffset();
        int SingleFileNum = totalFiles / totalThreads;
        String NNADDR = ToolConfig.getInstance().getHost();
        String HDFSDIR = ToolConfig.getInstance().getWorkPath();
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch Latch = new CountDownLatch(totalThreads);

        try {
            for (int i = 0; i < totalThreads; i++) {
                int FileStartNum = i * SingleFileNum + offset;
                int FileEndNum = (i + 1) * SingleFileNum;
                //hadoop每个文件夹都有文件数量上限，所以此处为每个线程执行的上传新建一个目录
                String dst = NNADDR + HDFSDIR + "/Thread-" + i + "/";
                subTask subTask = new subTask(dst, FileStartNum, FileEndNum, conf, Latch);
                ThreadPool.execute(subTask);
            }
            Latch.await();
            ThreadPool.shutdown();
        } catch (InterruptedException e) {
            log.warn("CheckStatusTask: execute task error,exception:", e);
        }
    }

    class subTask implements Runnable {
        private String dst;
        private int FileStartNum;
        private int FileEndNum;
        private Configuration conf;
        private CountDownLatch latch;

        public subTask(String dst, int fileStartNum, int fileEndNum, Configuration conf, CountDownLatch latch) {
            this.dst = dst;
            FileStartNum = fileStartNum;
            FileEndNum = fileEndNum;
            this.conf = conf;
            this.latch = latch;
        }

        @Override
        public void run() {
            System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            try {
                FileSystem fs = FileSystem.get(conf);
                for (int n = FileStartNum; n < FileEndNum; n++) {
                    String tmpdst = dst + n;
                    boolean ret = checkFile(tmpdst, fs);
                    if (!ret) {
                        log.warn("Check Status : check file error,file:" + tmpdst);
                    }
                }
            } catch (IOException e) {
                log.error("read task exception:", e);
            } finally {
                latch.countDown();
            }

        }
    }

}
