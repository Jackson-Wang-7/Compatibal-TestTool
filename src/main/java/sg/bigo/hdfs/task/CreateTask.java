package sg.bigo.hdfs.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.bigo.hdfs.common.HDFSConfig;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sg.bigo.hdfs.common.HdfsOperator.*;

public class CreateTask implements Runnable {

    final static Logger log = LoggerFactory.getLogger(CreateTask.class);

    private String src;
    private String dst;
    private int FileStartNum;
    private int FileEndNum;
    private Configuration conf;
    private CountDownLatch Latch;

    public CreateTask(String src, String dst, int fileStartNum, int fileEndNum, Configuration conf, CountDownLatch latch) {
        this.src = src;
        this.dst = dst;
        FileStartNum = fileStartNum;
        FileEndNum = fileEndNum;
        this.conf = conf;
        Latch = latch;
    }

    public static void doTask(Configuration conf) {
        int totalFiles = HDFSConfig.getInstance().getTotalFiles();
        int totalThreads = HDFSConfig.getInstance().getTotalThreads();
        int offset = HDFSConfig.getInstance().getFileOffset();
        int SingleFileNum = totalFiles / totalThreads;
        String NNADDR = HDFSConfig.getInstance().getHost();
        String HDFSDIR = HDFSConfig.getInstance().getWorkPath();
        String src = HDFSConfig.getInstance().getPutFilePath();
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch Latch = new CountDownLatch(totalThreads);

        try {
            for (int i = 0; i < totalThreads; i++) {
                int FileStartNum = i * SingleFileNum + offset;
                int FileEndNum = (i + 1) * SingleFileNum;
                //hadoop每个文件夹都有文件数量上限，所以此处为每个线程执行的上传新建一个目录

                String dst = NNADDR + HDFSDIR + "/Thread-" + i + "/";
                CreateTask hlt = new CreateTask(src, dst, FileStartNum, FileEndNum, conf, Latch);
                ThreadPool.execute(hlt);
            }
            Latch.await();
            ThreadPool.shutdown();
        } catch (InterruptedException e) {
            log.warn("CreateTask: execute task error,exception:", e);
        }
    }

    @Override
    public void run() {
        System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        try {
            FileSystem hdfs = FileSystem.get(conf);
            String putFilePrefix = HDFSConfig.getInstance().getWriteFilePrefix();
            for (int n = FileStartNum; n < FileEndNum; n++) {
                String tmpdst = dst + putFilePrefix + n;
                boolean ret = putToHDFS(src, tmpdst, hdfs);
                if (!ret) {
                    log.warn("write : put file to hdfs failed, file:" + tmpdst);
                }
            }
        } catch (IOException e) {
            log.error("write task exception:", e);
        } finally {
            Latch.countDown();
        }

    }
}
