package sg.bigo.hdfs.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.bigo.hdfs.common.HDFSConfig;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sg.bigo.hdfs.common.HdfsOperator.*;

public class ReadTotalTask implements Runnable {

    final static Logger log = LoggerFactory.getLogger(ReadTotalTask.class);

    private String dst;
    private List<String> paths;
    private Configuration conf;
    private CountDownLatch latch;

    public ReadTotalTask(String dst, List<String> paths, Configuration conf, CountDownLatch latch) {
        this.dst = dst;
        this.paths = paths;
        this.conf = conf;
        this.latch = latch;
    }

    public static void doTask(Configuration conf) {
        int totalThreads = HDFSConfig.getInstance().getTotalThreads();
        String NNADDR = HDFSConfig.getInstance().getHost();
        String HDFSDIR = HDFSConfig.getInstance().getWorkPath();
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch Latch = new CountDownLatch(totalThreads);

        try {
            FileSystem hdfs = FileSystem.get(conf);
            List<String> paths = getListing(NNADDR + HDFSDIR, hdfs);
            int SingleFileNum = paths.size() / totalThreads;
            for (int i = 0; i < totalThreads; i++) {
                List<String> targetPaths = paths.subList(i * SingleFileNum, (i + 1) * SingleFileNum);
                //hadoop每个文件夹都有文件数量上限，所以此处为每个线程执行的上传新建一个目录
                String dst = NNADDR + HDFSDIR;
                ReadTotalTask hlt = new ReadTotalTask(dst, targetPaths, conf, Latch);
                ThreadPool.execute(hlt);
            }
            Latch.await();
            ThreadPool.shutdown();
        } catch (InterruptedException | IOException e) {
            log.warn("CheckStatusTask: execute task error,exception:", e);
        }
    }

    @Override
    public void run() {
        System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        try {
            FileSystem hdfs = FileSystem.get(conf);
            for (int n = 0; n < paths.size(); n++) {
                String tmpdst = paths.get(n);
//                boolean ret = readFile(tmpdst, hdfs);
                boolean ret = readFile(tmpdst, hdfs);
                if (!ret) {
                    log.warn("read file error,file:" + tmpdst);
                }
            }
        } catch (IOException e) {
            log.error("read task exception:", e);
        } finally {
            latch.countDown();
        }

    }
}
