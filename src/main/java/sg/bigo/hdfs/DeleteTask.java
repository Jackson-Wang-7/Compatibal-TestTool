package sg.bigo.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static sg.bigo.hdfs.HdfsOperator.*;

public class DeleteTask implements Runnable{
    final static Log log = LogFactory.getLog(DeleteTask.class);

    private List<String> deletePaths;
    private Configuration conf;
    private CountDownLatch Latch;

    public DeleteTask(List<String> deletePaths, Configuration conf, CountDownLatch latch) {
        this.deletePaths = deletePaths;
        this.conf = conf;
        Latch = latch;
    }

    @Override
    public void run() {
        System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        try {
            FileSystem hdfs = FileSystem.get(conf);
            for (int i = 0; i < deletePaths.size(); i++) {
                String src = deletePaths.get(i);
                FileStatus fileInfo = getFileInfo(src, hdfs);
                if (fileInfo.isFile()) {
                    delete(src, hdfs);
                } else {
                    List<String> paths = getListing(src, hdfs);
                    for (String path : paths) {
                        delete(path, hdfs);
                    }
                }
            }
            Latch.countDown();
        } catch (IOException e) {
            log.error("exception:", e);
        }
    }

}
