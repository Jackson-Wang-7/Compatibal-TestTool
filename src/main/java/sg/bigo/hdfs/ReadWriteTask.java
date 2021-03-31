package sg.bigo.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static sg.bigo.hdfs.HdfsOperator.*;

public class ReadWriteTask implements Runnable {
    final static Log log = LogFactory.getLog(ReadWriteTask.class);

    //测试文件地址
    private static String SRC = "./testfile";

    private String dst;
    private int FileStartNum;
    private int FileEndNum;
    private Configuration conf;
    private CountDownLatch Latch;

    public ReadWriteTask(String dst, int fileStartNum, int fileEndNum, Configuration conf, CountDownLatch latch) {
        this.dst = dst;
        FileStartNum = fileStartNum;
        FileEndNum = fileEndNum;
        this.conf = conf;
        Latch = latch;
    }

    @Override
    public void run() {

        System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        try {
            FileSystem hdfs = FileSystem.get(conf);
            for (int n = FileStartNum; n < FileEndNum; n++) {
                String tmpdst = dst + n;
                boolean ret = putToHDFS(SRC, tmpdst, hdfs);
                if (!ret) continue;
                ret = checkFile(tmpdst, hdfs);
                if (!ret) continue;
                String tmpmvdst = tmpdst + "_re";
                ret = rename(tmpdst, tmpmvdst, hdfs);
                if (!ret) continue;
                ret = checkFile(tmpmvdst, hdfs);
                if (!ret) continue;
                delete(tmpmvdst, hdfs);
            }
            Latch.countDown();
        } catch (IOException e) {
            log.error("exception:", e);
        }
    }


}
