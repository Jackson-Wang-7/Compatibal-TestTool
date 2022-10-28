package com.wyy.tool.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static com.wyy.tool.common.ToolOperator.*;

public class DeleteTask extends AbstractTask {
    final static Logger log = LoggerFactory.getLogger(DeleteTask.class);

    private static volatile Queue queue = new ArrayBlockingQueue<String>(10000);

    public DeleteTask(Configuration conf) {
        super(conf);
    }

    public void doTask() {
        String deletePathPrefix = ToolConfig.getInstance().getDeleteFilePrefix();
        int totalThreads = ToolConfig.getInstance().getTotalThreads();
        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch latch = new CountDownLatch(totalThreads);

        try {
            FileSystem hdfs = FileSystem.get(conf);
            List<String> targetToDelete = listPrefix(deletePathPrefix, hdfs);
            for (String target : targetToDelete) {
                queue.add(target);
            }

            for (int i = 0; i < totalThreads; i++) {
                SubTask task = new SubTask(conf, latch);
                ThreadPool.execute(task);
            }
            latch.await();
        } catch (Exception e) {
            log.warn("DeleteTask: delete task error, exception:", e);
        } finally {
            ThreadPool.shutdown();
        }

    }

    class SubTask implements Runnable {
        private Configuration conf;
        private CountDownLatch latch;

        public SubTask(Configuration conf, CountDownLatch latch) {
            this.conf = conf;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
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
            } finally {
                latch.countDown();
            }
        }
    }

}
