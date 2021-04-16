package sg.bigo.hdfs.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import sg.bigo.hdfs.common.HDFSConfig;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static sg.bigo.hdfs.common.HdfsOperator.*;

public class DeleteTask implements Runnable {
    final static Log log = LogFactory.getLog(DeleteTask.class);

    private static volatile Queue queue = new ArrayBlockingQueue<String>(10000);
    private Configuration conf;
    private CountDownLatch latch;

    public DeleteTask(Configuration conf, CountDownLatch latch) {
        this.conf = conf;
        this.latch = latch;
    }

    public static void doTask(Configuration conf) {
        String deletePathPrefix = HDFSConfig.getInstance().getDeleteFilePrefix();
        int totalThreads = HDFSConfig.getInstance().getTotalThreads();

        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch latch = new CountDownLatch(totalThreads);

        try {
            FileSystem hdfs = FileSystem.get(conf);
            List<String> targetToDelete = listPrefix(deletePathPrefix, hdfs);
            for (String target : targetToDelete) {
                queue.add(target);
            }

            for (int i = 0; i < totalThreads; i++) {
                DeleteTask task = new DeleteTask(conf, latch);
                ThreadPool.execute(task);
            }
            latch.await();
            ThreadPool.shutdown();
        } catch (Exception e) {
            log.warn("DeleteTask: delete task error, exception:", e);
        }

    }

    @Override
    public void run() {
        System.setProperty("HADOOP_HOME", "/usr/hdp/3.1.0.0-78/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        while (!queue.isEmpty()) {
            String target = (String) queue.poll();
            if (target == null) {
                break;
            }

            try {
                FileSystem hdfs = FileSystem.get(conf);
                boolean ret = delete(target, hdfs);
                if (!ret) {
                    log.warn("DeleteTask: delete task failed, file:" + target);
                }
            } catch (IOException e) {
                log.error("delete task exception:", e);
            }
        }
        latch.countDown();
    }

}
