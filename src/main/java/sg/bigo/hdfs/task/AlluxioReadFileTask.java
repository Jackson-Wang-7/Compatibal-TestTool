package sg.bigo.hdfs.task;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.OpenDirectoryException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.bigo.hdfs.common.HDFSConfig;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sg.bigo.hdfs.common.HdfsOperator.readFile;

public class AlluxioReadFileTask implements Runnable {

    final static Logger log = LoggerFactory.getLogger(AlluxioReadFileTask.class);

    private String dst;
    private int FileStartNum;
    private int FileEndNum;
    private Configuration conf;
    private CountDownLatch latch;

    public AlluxioReadFileTask(String dst, int fileStartNum, int fileEndNum, Configuration conf, CountDownLatch latch) {
        this.dst = dst;
        FileStartNum = fileStartNum;
        FileEndNum = fileEndNum;
        this.conf = conf;
        this.latch = latch;
    }

    public static void doTask(Configuration conf) {
        int totalFiles = HDFSConfig.getInstance().getTotalFiles();
        int totalThreads = HDFSConfig.getInstance().getTotalThreads();
        int offset = HDFSConfig.getInstance().getFileOffset();
        int SingleFileNum = totalFiles / totalThreads;
        String NNADDR = HDFSConfig.getInstance().getHost();
        String HDFSDIR = HDFSConfig.getInstance().getWorkPath();
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch Latch = new CountDownLatch(totalThreads);

        try {
            for (int i = 0; i < totalThreads; i++) {
                int FileStartNum = i * SingleFileNum + offset;
                int FileEndNum = (i + 1) * SingleFileNum;
                //hadoop每个文件夹都有文件数量上限，所以此处为每个线程执行的上传新建一个目录
                String dst = NNADDR + HDFSDIR + "/Thread-" + i + "/";
                AlluxioReadFileTask hlt = new AlluxioReadFileTask(dst, FileStartNum, FileEndNum, conf, Latch);
                ThreadPool.execute(hlt);
            }
            Latch.await();
            ThreadPool.shutdown();
        } catch (InterruptedException e) {
            log.warn("CheckStatusTask: execute task error,exception:", e);
        }
    }

    @Override
    public void run() {
        System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        FileSystem fs = FileSystem.Factory.get();
        String putFilePrefix = HDFSConfig.getInstance().getWriteFilePrefix();
        for (int n = FileStartNum; n < FileEndNum; n++) {
            String tmpdst = dst + putFilePrefix + n;
            long TTL;
            try {
                long start = System.currentTimeMillis();
                AlluxioURI srcPath = new AlluxioURI(tmpdst);
                byte[] b = new byte[1024];
                int total = 0;
                int length;

                FileInStream in = fs.openFile(srcPath);
                while ((length = in.read(b)) > 0) {
                    total = total + length;
                }
                TTL = System.currentTimeMillis() - start;
                log.info("get file length[{}] ttl [{}]", total, TTL);
            } catch (Exception e) {
                log.error("read file " + tmpdst + " failed! ", e);
                TTL = -1;
            }
        }
        latch.countDown();

    }
}
