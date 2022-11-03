package com.wyy.tool.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.wyy.tool.common.ToolOperator.*;

@Deprecated
public class DiffTask implements Runnable {

    final static Logger log = LoggerFactory.getLogger(DiffTask.class);

    private String hdfsFilePath;
    private String localFilePath;
    private int FileStartNum;
    private int FileEndNum;
    private Configuration conf;
    private CountDownLatch latch;

    public DiffTask(String hdfsFilePath, int fileStartNum, int fileEndNum, String localFilePath, Configuration conf,
                    CountDownLatch latch) {
        this.hdfsFilePath = hdfsFilePath;
        this.localFilePath = localFilePath;
        this.FileStartNum = fileStartNum;
        this.FileEndNum = fileEndNum;
        this.conf = conf;
        this.latch = latch;
    }

    public static void doTask(Configuration conf) {
        int totalFiles = ToolConfig.getInstance().getTotalFiles();
        int totalThreads = ToolConfig.getInstance().getTotalThreads();
        int offset = ToolConfig.getInstance().getFileOffset();
        int SingleFileNum = totalFiles / totalThreads;
        String nnPrefix = ToolConfig.getInstance().getHost();
        String workDir = ToolConfig.getInstance().getWorkPath();
        String localFilePath = ToolConfig.getInstance().getCreateFilePath();
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch latch = new CountDownLatch(totalThreads);

        try {
            for (int i = 0; i < totalThreads; i++) {
                int FileStartNum = i * SingleFileNum + offset;
                int FileEndNum = (i + 1) * SingleFileNum;

                String hdfspath = nnPrefix + workDir + "/Thread-" + i + "/";
                DiffTask hlt = new DiffTask(hdfspath, FileStartNum, FileEndNum, localFilePath, conf, latch);
                ThreadPool.execute(hlt);
            }
            latch.await();
            ThreadPool.shutdown();
        } catch (InterruptedException e) {
            log.warn("DiffTask: execute task error,exception:", e);
        }
    }

    @Override
    public void run() {
        System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        try {
            long localfileLth = getLocalFileSize(localFilePath);
            if (localfileLth == -1) {
                return;
            }
            String localfileMd5 = getLocalFileMd5(localFilePath);
            log.warn("local file md5 is {},file length is {}", localfileMd5, localfileLth);
            FileSystem hdfs = FileSystem.get(conf);
            String putFilePrefix = ToolConfig.getInstance().getCreateFilePrefix();
            for (int n = FileStartNum; n < FileEndNum; n++) {
                String tmpHdfsFilePath = hdfsFilePath + putFilePrefix + n;
                FileStatus fileInfo = getFileInfo(tmpHdfsFilePath, hdfs);
                if (fileInfo != null && fileInfo.getLen() == localfileLth) {
                    boolean ret = diffMd5(tmpHdfsFilePath, localfileMd5, hdfs);
                    if (!ret) {
                        log.error("diff task failed, file: " + tmpHdfsFilePath);
                    }
                } else {
                    log.error("file {} is in inconsistent state ", tmpHdfsFilePath);
                }
            }
        } catch (IOException e) {
            log.error("diff task meets exception:", e);
        } finally {
            latch.countDown();
        }
    }
}
